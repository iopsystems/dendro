---
status: open
opened: 2026-09-24
updated: 2026-09-25
---

# Read-optimized finished archives: a ladder from today's file to a flat layout

## Goal

Decide how an archive that is *done* should be laid out and read, so that
the needs of reading stop being served by a layout chosen for recording.

The format has two phases already: rows land in the `wal` table, and a seal
turns a batch of them into an immutable parquet segment. The sealed phase
inherited the live phase's container, so a finished archive is a SQLite file
with 4 KiB pages whose segments are overflow-page chains, whose catalog says
nothing about what a stream holds, and whose manifest sits at no fixed
offset. Every reader pays for that in ways the writer never sees.

This entry lays out five independent steps toward a read-optimized finished
archive, each with its own measurable effect, in the order that each pays
back before the next is needed. The first three change nothing about the
container. The fourth changes only a copy. The fifth, a flat layout beside
the SQLite one, is designed here and gated. An adversarial review of the
first draft (2026-09-24, recorded under Evidence) reordered the ladder: the
draft led with the flat layout and had not priced a read-optimized *SQLite*
copy against it.

Intent-first: this record lands before any implementation.

## Decision Criteria

**Precondition for everything below: a consumer opens dendro archives.**
Today none does. rezolus reads `.rez` through its own `crates/rez`, which
never checks the `dend` stamp, and systemslab reads through rezolus. The
measured costs under Evidence are real and attach to `.rez` files, whose
catalog is this crate's; they attach to dendro's files only once rezolus
writes them (rezolus #1224) or lands the same additions in `crates/rez` in
parallel. Either is a gate, and it comes first.

**GO on step 1 (stream summary) and step 2 (header) now**, given the
precondition. Both are additive under FORMAT.md §8 and each removes a read
cost that was measured on real archives.

**GO on step 3 (incremental blob reads) now.** It is a per-segment handle
and a change to `probe`, and its effect is memory rather than I/O
(Evidence); it is the smallest step and the one step 5 makes unnecessary for
itself.

**GO on step 4 (the read-optimized copy and a range VFS)** when a consumer
wants to render from an archive it has not downloaded, measured as bytes
fetched for a first render against bytes the render needed, or seconds to
first render against the whole-file download it replaces. One precondition
is outside this crate: the store must serve the file as stored. systemslab
compresses artifacts at rest when `storage.compression` is on and advertises
`Accept-Ranges` only for uncompressed ones, so `.rez` artifacts need an
exemption first.

**GO on step 5 (the flat layout)** only when a consumer must read without
SQLite, or when a measured read through step 4 still fails the step-4
metric. Nothing today asks for either: the browser viewer already compiles
SQLite in.

**NO-GO on the container learning what a row, an index entry, or a summary
says.** Every new slot below is opaque bytes the caller writes and dendro
carries. The scope test from [the survey](2026-09-12-what-a-tsdb-has-that-we-do-not.md)
applies: two callers with different row shapes must both be able to use it.

**NO-GO on touching the live file's geometry.** `PAGE_SIZE` is 4096 because
it is the one cost paid on every tick (`DESIGN.md`: 3.14x sidecar write
amplification against 8.20x at 64 KiB). Step 4 changes the page size of a
*copy*, which SQLite permits, and leaves the writer's file alone.

## Scope

In: the five steps, what each measurably changes, what each costs, and the
alternatives set aside at each rung. The flat layout's design, so that step
5 is a build decision and not a design one when its gate is met.

Out: compaction and retention, which are the live form's and stay there;
what a row, a `caller_index`, a `caller_rows` blob or a summary *means*,
which is the encoder's; pre-rendered dashboard tiles, which are a consumer's
cache and not a container's concern, though they move dashboards more than
any layout does.

## Evidence

Items marked *measured* were run on 2026-09-23/24: the rezolus figures
against `crates/rez` on real archives, the SQLite figures on scratch files
with the macOS `sqlite3` 3.51.0 during the adversarial review. Items marked
*documented* cite SQLite's file-format or pragma documentation. Two items are
from memory of `btree.c` and say so.

**Opening reads every segment's footer, and a footer read is a whole-blob
read** (*measured*). The reader's store answers a segment request with
`SELECT bytes FROM segments` (rezolus `rez_sqlite.rs:753`; this crate's
`read::probe`, `read.rs:222-235`, is the same shape). To learn a stream's
column names at open, the reader pulls one full segment through memory and
parses its footer. On a 1.28 GB, 9.6-hour archive whose per-task stream had
159 segments of up to 2,851 columns, that was the whole of the viewer's
4.1 GB resident size before segments were made on-demand, and 1.37 ms per
segment over 418 segments of which a typical query touched 11%. Nothing in
the catalog says what a stream holds; only the segment does.

**A consumer downloads 200 MB to learn a source's name** (*measured*).
systemslab's artifact metadata endpoint, asked which viewer to route a
`.rez` to, streams the whole object to a temp file and opens it, because the
manifest is a row in a SQLite table at no fixed offset
(`crates/server/state/src/artifact/fetch.rs:459`, `import_metrics.rs:257`).
The first 100 bytes say "SQLite, application id `dend`" and nothing more. The
endpoint's own documentation still says "only the footer is fetched".

**The identity index is an event log every reader replays** (*measured*).
rezolus keeps slot-to-series transitions in `caller_rows` and restates the
full slot set every 300 s so retention can cut history. A reader replays from
the last restatement, diffing per entry. On a ten-minute recording under task
churn (248,511 entries) the replay took 20 s before an algorithmic fix and
0.6 s after (rezolus #1282). Every consumer wants intervals; the file stores
events.

**Overflow pages** (*documented*). A BLOB larger than a leaf cell's local
share is a chain of overflow pages, each `usable_size - 4` bytes of content
behind a 4-byte pointer to the next. A 4 MB segment at 4096 is about 1,025
pages; at 65536, 64.

**A live buffer fragments its segments; an exact copy defragments them**
(*measured*). Sixteen 1 MB blobs after delete/insert churn plus
`incremental_vacuum` were each split into two or three runs. The same file
after `VACUUM INTO` had every blob in one run, interrupted only where a
pointer-map page falls (every 819 pages at 4096). `VACUUM INTO` also honored
a `PRAGMA page_size = 65536` issued first: the copy came out at 64 KiB pages
with `auto_vacuum = 2` preserved and root pages unchanged. The copy is in
rollback-journal mode, not WAL (header bytes 18/19 were 1), which reads do
not notice (`open_bytes` already rewrites 2 to 1, `archive.rs:816-830`) and a
writer reopening a copy would have to re-set.

**A recorder's finished archive is mostly contiguous already** (reasoned
from the above, not measured on a real archive). Each seal inserts one blob
in one transaction, so a segment's pages are allocated together; only a
buffer that has evicted and reused pages fragments. Two streams sealing in
turn interleave their blobs, not their pages.

**Root page 3 holds** (*measured*, 3.51.0). A table created first in a
database with `auto_vacuum = INCREMENTAL` got root page 3 (page 2 being the
first pointer-map page, *documented*), and kept it through churn,
`incremental_vacuum`, and `VACUUM INTO`. *From memory of `btree.c`*:
`incrVacuumStep` moves only the file's last page into a free slot and never
a root page, which is consistent with what was measured and is not a
documented guarantee. With the real `SCHEMA_SQL`, page 1 held ten
`sqlite_master` rows totaling 1,755 bytes of DDL, because comments inside a
`CREATE TABLE`'s parentheses are stored verbatim (the `sources` row alone is
521 bytes), leaving about 1.4 KB free: room for two more tables and their
autoindex rows, with little to spare.

**Incremental BLOB I/O saves memory, not page reads, on a fragmented
chain** (*from memory of `btree.c`*). Seeking within a blob follows the
chain; on an autovacuum database the pointer map lets it skip loading pages
of a contiguous chain, but a fragmented chain is followed page by page. So
on the rolling buffer, a footer read through `blob_open` avoids the 4 MB
allocation and the parquet parse, and not the thousand page reads.

**Snapshots copy everything** (*documented*). `Archive::vacuum_into` is the
exact copy of a live archive and the right one; it rewrites every page. It is
not fsynced, per the `VACUUM INTO` documentation.

**Footers are most of a wide segment, and the embedded Arrow schema is most
of the footer** (*measured*, 2026-09-25). Two real recordings, converted from
`.rez` with rezolus's `recording upgrade --to dendro` (segment bytes copied
unchanged), read with dendro's API in release mode, warm page cache, three
runs each:

| | 1.28 GB, 9.6 h, older agent | 581 MB, 2.3 h, rezolus 5.22.0 |
|---|---|---|
| segments | 5,874 | 1,467 |
| `read::catalog` | 7 ms, 28 MB peak RSS | 2 ms, 13 MB peak |
| `probe` every stream | 8 ms, 4.5 MB pulled | 5 ms, 5.6 MB pulled |
| every segment's footer | 0.43 s, 1,272 MB pulled, 110 MB peak | 0.28 s, 579 MB pulled, 329 MB peak |
| footer bytes of those | 529 MB (42%) | 423 MB (73%) |

The share is set by width. The per-task stream was 49% footer at 2,325
columns per segment in the older recording and 88% at 13,941 in the 5.22.0
one; narrow streams were 1–3%. The largest 5.22.0 segment held 37,644
columns and 301 rows: 35.6 MB, of which the footer was 32.1 MB and its
`ARROW:schema` key-value entry 27.5 MB. That entry is the serialized Arrow
schema with every field's metadata, which for this stream is every task
slot's labels, repeated in every segment. The 5.22.0 writer opens a new
column when a slot changes occupant within a segment, which is what took
the width from hundreds to tens of thousands.

Identifying the archive needed little: on both files, and on the `.rez`
files they came from, the `sources` row sat on page 3 with overflow pages up
to 15 or 17, so a 60–68 KB prefix held it. The row itself was 14–21 KB,
mostly the caller's system description and metric help text.

**The review that reordered this entry.** The first draft led with a flat
layout and argued against an HTTP VFS on the live file's 4 KiB fragmented
chains, without pricing a VFS over a defragmented 64 KiB copy. The review
measured the copy, found the page-3 argument sound but its `sqlite_master`
step dependent on page 1 staying a single leaf, found `as_of_seq` unusable
(a filtered copy renumbers `seq` and `SourceWriter::seal` returns `()`),
found the range trait's synchronous shape incompatible with browser `fetch`
outside a worker, and found that "never re-encodes" is false for an export of
a live tail. Each is addressed below.

## Design and Implementation

### The ladder

| step | what changes | what it removes | reader needs | container |
|---|---|---|---|---|
| 1 | `stream_summary` | the footer probe per stream at open; the identity replay on read | a catalog read | unchanged |
| 2 | `header` table on root page 3 | the whole-file fetch to route a link | 12 KiB | unchanged |
| 3 | incremental blob reads | the 4 MB allocation per footer probe | SQLite | unchanged |
| 4 | `VACUUM INTO` at 64 KiB + a caching range VFS | the whole-file fetch to render | SQLite + a VFS | unchanged |
| 5 | a flat layout with a trailing catalog | SQLite from the reader; requests per segment 1 to 4 → 1 | a range reader | second form |

The property that decides between 4 and 5 is whether the reader may carry
SQLite. Bytes moved are equal; requests differ by a small constant; nothing
else differs. Where dashboards are concerned, neither changes the fact that a
first render touches most streams: what moves dashboards is step 1 (the
section list and each plot's shape from the catalog, no footers) and a
consumer-side tile cache, in that order, and then the page cache in step 4
that makes a second render local.

### Step 1: what a stream is known to hold

A caller-owned, stream-level, opaque blob: the stream analog of
`caller_index`, with the opposite lifetime. `caller_index` describes one
segment and is dropped by a merge and by a projection, correctly, because an
index over one input cannot describe two. A summary describes the *stream*
and survives compaction; a projection drops it (same rule, same reason); a
seal or a resume that changes what the stream holds replaces it.

Three slots could hold it, and the entry has to choose with reasons:

- **`sources.metadata`** via `update_metadata`: already copied, replicated in
  `Handshake.metadata`, returned by `read::catalog` with no payload read.
  Against: it is one JSON string map per *source*, so a per-stream summary
  for 49 streams becomes one large value, and `patch_source_metadata` is a
  read-modify-write of the whole map, so a per-seal refresh rewrites
  everything per seal. A projection copies it verbatim, so a stale summary
  would describe columns the copy no longer has.
- **`caller_rows` under a reserved stream name**: time-keyed, untouched by
  compaction, copied by range, replicated by `Index`, "latest wins" by
  `ORDER BY ts DESC LIMIT 1`. Against: a projection copies it verbatim too,
  so the same staleness applies unless the container special-cases a name,
  which is a rule about a row's meaning. And retention evicts it by `ts`, so
  a summary for a stream with no recent seal is evicted with its old rows
  while the stream still has segments.
- **A table**, with the lifetime rule stated:

  ```sql
  CREATE TABLE stream_summary(
    source_id INTEGER NOT NULL REFERENCES sources(id),
    stream TEXT NOT NULL,
    blob BLOB NOT NULL,                  -- opaque, the caller's
    as_of_ts INTEGER NOT NULL,           -- last_ts of the newest segment it describes
    PRIMARY KEY (source_id, stream)
  );
  ```

  `as_of_ts`, not a `seq`: a filtered copy renumbers `seq` densely from 0,
  `SourceWriter::seal` does not report the seq it produced, and a seq means
  nothing after retention evicts below it. A timestamp survives all three and
  is what the caller has.

The table is the recommendation. It is the only slot whose copy rules can be
stated without the container reading anything, and it gives `read::catalog`
a column rather than a convention. Its cost is one frame kind for
replication (`rewrite`'s completeness test, `rewrite.rs:747-770`, refuses a
new table that is neither copied nor deliberately dropped, and the frame set
rests on that same test) and a writer message on the ticks channel, so a
caller can refresh after every seal or only at finalize.

The lifetime rules now form a table worth putting in FORMAT.md, so the next
slot is a row in it rather than a fourth table:

| slot | keyed by | compaction | projection | retention |
|---|---|---|---|---|
| `caller_index` | segment | dropped | dropped | with its segment |
| `caller_rows` | time | untouched | untouched | by `ts`, or from a caller's floor |
| `stream_summary` | stream | untouched | dropped | with its stream |

**Staleness on a live archive.** The summary describes segments up to
`as_of_ts`; the live tail can hold columns it lacks (rezolus's task streams
change schema on most scrapes). A reader of a live archive with a non-empty
tail therefore uses summary ∪ tail probe, and `read::probe` on the sealed
segments is what a reader does only for a stream with no summary. On a
finished archive the summary is complete.

What rezolus would put in it: metric names and kinds, each column's field
metadata (what the segment footers repeat today; see Evidence), cadence, and
its identity history as occupancy intervals. Not a series count: rezolus
5.22.1 removed the only consumer of one. The caller replays its own event log once, at seal or
at finalize, and stores the intervals where every reader finds them; the log
stays the source of truth and the restatement mechanism stays for the rolling
buffer. No reader of a finished archive runs the replay.

### Step 2: a range-readable header

Page 1 is `sqlite_master`, page 2 the pointer map, and the first table created
gets root page 3 (Evidence). A table whose single row fits in one page keeps
its cell on its root page, and an in-place `UPDATE` that stays under the
page limit keeps it there.

```sql
CREATE TABLE header(
  id INTEGER PRIMARY KEY CHECK (id = 1),
  body BLOB NOT NULL                   -- opaque, the caller's, under 2 KiB
);
```

created first in `SCHEMA_SQL`. Four rules the first draft lacked:

- **Owner.** The header is per *archive*, so it is written through a
  `Writer`-level message, not at `add_source`. A caller sets it once and may
  replace it; `finalize` is where a recorder writes the final one.
- **Copies.** `rewrite`'s combine has two inputs and one output: the caller
  supplies the output's header, or the output has none. The table is listed
  as deliberately not carried in the completeness test, so a copy never
  silently inherits a header that described a different archive.
- **Self-validating body.** The body begins with its own magic, a length,
  and a CRC32 of the rest, so `sniff_prefix` can accept it *without* the
  `sqlite_master` step when that page is not a single leaf. The
  `sqlite_master` check is confirmation, not the only path, and the DDL
  comments should be stripped from the stored schema to keep page 1's margin
  (Evidence: 1.4 KB left today).
- **Staleness.** On a live buffer an `UPDATE header` sits in the `-wal`
  sidecar until a checkpoint, so a sniff of the main file is up to
  `CHECKPOINT_INTERVAL` stale. The stamp needed an explicit checkpoint for
  the same reason (`archive.rs:632-641`). The finalize write is covered by
  the close checkpoint.

`sniff_prefix(bytes)` takes the first `3 * PAGE_SIZE` bytes, decodes the one
table-leaf cell on page 3 without a SQLite library, refuses a cell with an
overflow pointer, validates the body's CRC, and returns it. A test writes an
archive, truncates the file to three pages, and sniffs it; it also pins that
page 3 is still the root after churn, vacuum, and `VACUUM INTO`. The
invariant is tested rather than documented, and the fallback is the sniff
saying "no header", never a wrong one.

Older archives sniff as they do today. The step-5 layout has its header at
offset 0 by construction and needs none of this; the live buffer is the case
that does, and it is the one that was measured.

### Step 3: read a footer without pulling the blob

`SegmentBytes` is a stream-level type today (`read.rs:444`; `all()` returns
a stream's payload list). A footer read needs a per-segment handle:
`SegmentHandle::tail(n)` and `range(offset, len)` over `blob_open`, with
`probe` using the first. On a contiguous chain this is a few page reads; on a
fragmented one it still follows the chain but skips the allocation and the
parse (Evidence).

**Worth less than this entry first assumed** (Evidence, 2026-09-25). A
footer read skips the data section and keeps the footer, and on wide streams
the footer is most of the segment: reading every footer of the 581 MB
recording would pull 423 MB instead of 579 MB. Step 1 removes those reads
from open altogether; this step matters only for a reader that still needs
a footer, and for narrow streams. The writer keeps its plain `INSERT` (`rez_sqlite.rs:1558`
records why `blob_open` is the wrong tool at write time). The rusqlite `blob`
feature is already on.

### Step 4: a read-optimized copy, and a VFS that reads it by range

`Archive::vacuum_into` gains a page-size argument, defaulting to 65536 for a
copy that will be read and not written. The result is a SQLite file with
contiguous segments of 64 pages per 4 MB, the same sniff, the same reader,
the same `verify`, and no new container. FORMAT.md §2.2 gains the sentence
that a copy is in rollback-journal mode and that `Writer::open` on one must
re-set WAL.

Reading it remotely is a VFS whose page read is a range request:

- opens the file as immutable (`immutable=1`: no `-shm`, no locks, and the
  file must have been checkpointed, which a clean finalize and any
  `VACUUM INTO` guarantee);
- fetches with readahead sized to the page geometry (a 4 MB segment at
  64 KiB pages is four requests at 1 MiB readahead);
- keeps a persistent page cache on local disk, so a first render fetches what
  it needs and a second render is local. The whole-file download systemslab
  does today is then the eager form of the same cache, and both models are
  one reader.

In the browser, the same VFS runs in a Web Worker, where synchronous XHR is
permitted; that is how sql.js-httpvfs reads ordinary databases over HTTP
today, at 4 KiB pages. The reader API stays synchronous; the async boundary
is the worker's.

This step works on today's uploaded archives without the copy, less well:
their segments are already mostly contiguous (Evidence) but at 4 KiB, so
sixteen times the pages per segment and a catalog whose rows are scattered
across pages allocated over the recording's life. The copy is what makes the
request count small and the fragmented buffer a non-case.

### Step 5: the flat layout

Designed so that the gate, when met, is a build decision.

```
offset 0      header      fixed 32 bytes: magic `dendro\0f`, form version u16,
                          schema version u32, reserved
              segments    each one parquet file, verbatim, 4096-aligned
              blobs       caller_index, caller_rows, stream_summary payloads
              catalog     the wire frame set with payloads replaced by ranges
end - 24      footer      catalog offset u64, catalog length u64,
                          CRC32 of the catalog u32, magic
```

- **The catalog is the replication frame set** (WIRE.md §4: `Handshake` for
  `sources`, `Segment`, `Index` for `caller_rows`, `ClockOffset`, plus a
  `StreamSummary` frame from step 1), encoded with WIRE.md §3's primitives,
  with every payload field replaced by an `(offset u64, length u64)` into
  this file. No msgpack, no serde: the codec exists and its completeness
  check is `rewrite`'s. `Rows` frames do not appear; a finished archive has
  no `wal`.
- **The digest is in the footer**, a CRC32 over the catalog, so the file is
  written front to back and can be streamed to a pipe or a multipart upload.
  It defends against truncation and a spliced catalog; a segment's own
  parquet footer defends its bytes, and `verify` checks each declared length
  against it.
- **Alignment is 4096** so a local reader can mmap or `O_DIRECT` a segment;
  at 4 MB segments the padding is under 0.1%.
- **Segments are written in `(source_id, stream, seq)` order** so one
  stream's segments are adjacent and a time-window read of one stream is one
  coalesced range; `caller_rows` blobs per stream in `ts` order for the same
  reason.
- **What produces one.** `Writer::finalize_into(path)`: seal every tail,
  then write the flat file from one read snapshot; this is the path for which
  "same bytes, never re-encoded" holds. `rewrite::export(live, path)` from an
  open `Archive` of a buffer another process is still writing materializes
  the live tail through the encoder into one more segment, exactly as
  `copy_sources_into` does today (`rewrite.rs:604-632`): that *is* an encode,
  it produces a segment the writer's next seal would not, and it needs the
  caller's encoder linked into the snapshot tool. Both are stated; neither is
  a problem.
- **Replication.** A flat archive can be a publisher source
  (`ArchivePublisher::catching_up` needs only the read API, and the file is
  a catch-up frame set) and never a subscriber target (`Subscriber` owns a
  `Writer`).
- **The reader.** `Archive::open` sniffs and opens either form. This is the
  largest cost the first draft omitted: `Archive` is a struct over a
  `Connection` with about sixty public methods, and `SegmentBytes::Shared`
  holds an `Arc<Mutex<Archive>>` in the public API. Either form becomes an
  enum or a trait with an arm or an error for `read_snapshot`, `page_stats`,
  `vacuum_into`, `verify`, `serialize`, every `pragma_*`, and the publisher.
  The `read::*` functions keep their signatures; the type under them does
  not.
- **`verify` parity.** A flat `verify` that checks segment lengths against
  parquet footers is stronger than the live form's, which never opens a
  segment (by design, so a `not-parquet` blob reads as sound). Either lift
  the live one or say so in `verify`'s report.

### Cost

**Steps 1 to 3**: a few days each side of the boundary. Step 1 is a table,
a frame kind, a writer message, two reader accessors, and the lifetime table
in FORMAT.md; step 2 is `SCHEMA_SQL` order, a cell decoder of about 150
lines, the CRC framing, and the truncation and root-page tests; step 3 is a
per-segment handle and a change to `probe`. None bumps `SCHEMA_VERSION`.
rezolus then fills the summary from its encoder and routes on the header.

**Step 4**: the page-size argument is an afternoon. The VFS is the work: a
`sqlite3_vfs` in Rust with readahead and a disk cache, a worker-hosted
variant for wasm, and the systemslab storage exemption. One to two weeks.

**Step 5**: the writer is small (the codec exists; offsets are bookkeeping).
The reader, the `Archive` split, `verify`, `describe`, an `export` CLI,
truncation fuzzing (a catalog length read off disk is an allocation request:
the `MAX_FRAME_BYTES` lesson), the wasm check, rezolus's `recording snapshot`
(today `vacuum_into`) and systemslab's `is_rez`, which tests the filename and
would need a content sniff or a second extension. Every read assertion in
`tests/{catalog,verify,identity,caller_index,caller_rows,roundtrip}.rs` runs
against both forms. Three to four weeks, and a doubled read-test matrix for
as long as both forms exist.

### Alternatives considered

Set aside, with why, so they are not re-derived:

- **The flat layout first** (the first draft). Rejected for now because
  step 4 delivers range reads with one container, and nothing today needs a
  SQLite-free reader.
- **A page-map catalog beside the copy**: record each segment's leaf page,
  first overflow page and page count after `VACUUM INTO`, so a reader can
  compute byte offsets and fetch overflow pages by plain range, stripping the
  4-byte pointer per page. Removes SQLite from the segment path but not from
  the open path, and the table must exist before the copy or its own pages
  relocate one. Worth revisiting if step 4's request count is the problem and
  step 5's cost is not yet justified.
- **Zip64 with stored entries** as the flat container: the central directory
  is the trailing catalog, CRC32 per entry, alignment via an extra field, and
  `unzip`, Python and browsers read it for free. Gives every property step 5
  lists; loses the frame-set completeness check and adds a dependency. The
  strongest alternative to the bespoke layout if step 5 is ever built, and
  the choice between them is made then.
- **The replication frame stream as a file** with a trailing offset index:
  step 5 *is* this with payloads moved out of line, so the two are one
  design; the out-of-line form is what makes a segment one range.
- **One parquet file per stream, one row group per segment**:
  `concat_parquet` already does this for compaction, and the catalog is
  parquet's own footer. Only for schema-stable streams and one stream per
  file unless wrapped, so it is a compaction output rather than a container.
- **Segments as files in a directory**, tar'd for transport. Parquet tooling
  works directly, snapshot is a hardlink, sync is incremental. Loses the
  one-file property the container is shaped around and needs the
  staging-and-rename protocol the README says SQLite exists to avoid.
- **Convert in the consumer.** systemslab already downloads and opens every
  metrics artifact during post-processing and could persist a source's name
  and a per-stream summary there. It fixes the 200 MB routing case for
  systemslab with no format change, and is worth doing in the interim; it
  fixes nothing for any other consumer, which is why steps 1 and 2 exist.
- **Larger pages for the live file.** Rejected by `DESIGN.md`'s
  measurement; step 4 gets the benefit on the copy.
- **Not viable**: the 20 reserved header bytes at offset 72 must be zero
  (documented); `user_version` is the schema version; `sqlar` has the same
  overflow layout as any BLOB; asar, RIFF and ISO-BMFF boxes are bespoke
  containers with no tooling gain over zip.
- **Do nothing.** Defensible: systemslab downloads once and caches, the
  browser uploads whole files, snapshots are rare. Steps 1 to 3 are worth
  doing regardless because their costs were measured on real archives.

## Outcome

Open. **Step 1 is built** (2026-09-25), ahead of the consumer precondition
by the decision recorded in rezolus's plan: rezolus's reader for dendro
archives is being built on this crate's API, and 6.0 archives should carry
summaries from their first write. It shipped as specified above, with two
changes found while building it: `Frame` became `#[non_exhaustive]`, and
`FrameReader` now skips an unknown frame kind as `WIRE.md` §7 already said
it did (it returned an error). A time-bounded copy drops a summary as well
as a projection, since it holds fewer segments than the summary describes.

Steps 2 and 3 are revised by the 2026-09-25 evidence. Step 2 saves no bytes
on the recordings measured, whose `sources` row already sits in the first
17 pages; what it would add is a fixed offset readable without SQLite. And
the row is 14–21 KB, so a 2 KiB header holds a subset of it, not a copy.
Step 3 saves less than assumed, since footers are most of a wide segment.
The largest lever the evidence shows is outside this crate: per-column
labels in every segment's `ARROW:schema`, which the summary lets rezolus
write once per stream instead. Steps 4 and 5 keep their gates.

## Deferred or Reopen Items

- **Step 5's gate**: a consumer that must read without SQLite, or a
  measured step-4 read that still fails the first-render metric.
- **The zip64 question** is deferred to the day step 5 is built; the design
  above is the bespoke form and the alternatives section records why zip64
  is its peer.
- **Fragmentation on a real buffer**: the "mostly contiguous" claim for a
  recorder's finished archive is reasoned, not measured. Measure segment
  contiguity on a real hindsight buffer and a real recorder archive before
  step 4 is priced against today's uploads.
- **Pre-rendered tiles** are a consumer's, but they change the step-4 gate:
  a dashboard whose first render is served from tiles fetches almost nothing
  from the archive, and the gate's metric should be measured with and
  without them.
- **A second encoder-side caller** (the [encoder boundary](2026-09-11-encoder-boundary.md)
  question) that cannot express its summary as an opaque blob would mean the
  slot is not general and the lifetime table needs a different shape.
- **SQLite changing root-page allocation** would fail the step-2 test; the
  sniff falls back to "no header" and step 5's header does not depend on it.

## Appendix: Skills Invoked

- `engineering-journal` — this entry's shape and lifecycle.
- `propose-design` — loaded for its questions (approach, why, what it looks
  like, cost, alternatives, what changes the analysis), which the sections
  above answer; its vault brief workflow was not used, since this repository
  keeps design records here.
