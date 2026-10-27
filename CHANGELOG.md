# Changelog

Notable changes per release. This file follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this crate follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html), with the caveat
that while the major version is 0 **any minor release may break**; see
*Before 1.0* below.

The minor is where a break goes, and that is Cargo's rule rather than a
preference: `dendro = "0.1"` resolves to `>=0.1.0, <0.2.0`, so every `0.1.z` is
compatible with every other and a patch release reaches existing callers on
their next `cargo update`. A removed public item therefore goes to `0.2.0`, not
`0.1.1` -- otherwise the break arrives as an upgrade nobody asked for.

The reasoning behind a change lives in [`docs/journal/`](docs/journal/README.md),
one entry per effort. This file says what changed; the journal says why, and
carries the measurements.

## Releasing

A release is one pull request and two workflows. The pull request, titled
`release: prepare vX.Y.Z`, sets `version` in `Cargo.toml`, renames the
`[Unreleased]` section below to `[X.Y.Z] - YYYY-MM-DD`, and adds a new empty
`[Unreleased]` above it. Merging it runs `tag-release.yml`, which checks that
the commit names the manifest version, creates the tag `vX.Y.Z`, and pushes
the next development version (`X.Y.(Z+1)-alpha.0`) to `main`. The tag runs
`release.yml`, which runs the checks CI runs, publishes the crate to
crates.io, and creates the GitHub release with this file's `[X.Y.Z]` section
as its notes. A version with no section here is refused. If that run fails,
fix the cause on `main` and run `release.yml` by hand with the tag as its
input; a tag push runs the workflow as it was at the tagged commit, so a fix
cannot reach a failed release any other way.

## [Unreleased]

### Added

- `Frame::SourceUpdate` (wire kind 6, WIRE.md §4.6): a source's labels and
  metadata after its handshake, both maps whole, and `ts`, the publisher's
  wall time when it saw the change. `Subscriber` replaces its copy's labels
  and metadata with them, except the copy's `encoder` and `writer_sessions`
  where it has them; any other key the publisher removed is removed. It
  counts them in `Applied::source_updates`. A `FrameReader` on dendro 0.3.0
  or later that does not know the kind skips it (WIRE.md §7);
  `wire::decode_payload` before this release refuses it.
- `SourceWriter::set_identity` and `ArchiveMut::set_source_identity`, which
  replace a source's labels and metadata in one transaction, keeping the
  source's `encoder` and `writer_sessions` where it has them, since they
  describe the file's own encoder and writer sessions; every other key,
  `events` included, is replaced. `Transaction::set_source_identity` writes
  both maps as given.

### Changed

- `ArchivePublisher::next` reads the archive's sources on every poll. It sends
  a `SourceUpdate` for a source whose labels or metadata changed since they
  were last sent, ahead of that source's other frames, and a `Handshake`,
  with the next ordinal, for a source created after the publisher opened,
  followed by what the opening catch-up would ship for it: segments and clock
  offsets from `catching_up`'s `since` (all of them for `tailing`), index
  entries and live rows. Before this the publisher read the sources once,
  when it opened, so a subscriber kept the identity it saw at connect and
  never received a source added later. A caller of `next` that assumed a
  fixed set of sources now sees handshakes after the first batch.

### Fixed

- `ArchivePublisher::next` ships the sealed rows of a stream it has shipped
  no row of as `Segment` frames. A stream with no segments when the
  publisher opened, or that did not exist then (or, under `catching_up`, one
  that held only live rows), sealed before a poll read its rows, lost those
  rows without an error: its cursor had nothing to compare the seal's
  watermark with, and the live tail held only the rows after the seal. Every
  stream of a source created after the publisher opened, and handshaken
  before it had rows, took that path.

## [0.6.0] - 2026-10-19

This is 0.6.0, not 0.5.2. Archives this release creates are schema version
5, which releases up to 0.5.1 refuse (`unsupported archive schema version
5`). A patch release would reach `dendro = "0.5"` callers on a `cargo
update`, and a process on the new build would then write files that their
other processes, still on the old one, cannot open: the same break-by-upgrade
the minor exists to prevent (see 0.2.0 below). `Error` gains variants, which
`#[non_exhaustive]` allows, and `Error::UnsupportedSchema` now reports
`reads: 4`.

### Added

- Archive-level documents: named JSON documents that belong to the archive
  rather than to a source, in a new `documents` table (FORMAT.md §3.6). A
  document is `(kind, name)` with a caller-chosen `schema` string naming its
  body's type and version. Writes append a revision and never overwrite one;
  the current revision is the newest, and a delete is a revision with no
  body. Written with `ArchiveMut::put_document`, `put_document_if_current`
  (which takes the uuid of the revision the caller read, and is refused as
  the new `Error::DocumentConflict` when another revision is current) and
  `delete_document`, which return the new revision's `archive::RevisionId`
  (id and uuid), so conditional saves chain without a read between them;
  the same three on `Transaction`, and on `Writer` for an archive being
  recorded, which an `ArchiveMut` cannot open. Read with
  `Archive::document`, `documents`, `document_kinds` and `document_history`
  (types `archive::{Document, Deletion, Revision}`). A body must be JSON
  (RFC 8259, no nesting limit). Kinds beginning `dendro.`
  (`archive::RESERVED_KIND_PREFIX`) are refused.
- `rewrite::copy_documents_into`, which copies every document revision of
  one archive into a destination transaction, skipping revisions the
  destination already holds (by uuid) and failing on one it holds with
  different contents. It returns `archive::CopiedDocuments`: the revisions
  inserted, the documents whose current revision came from this input, and
  which of those displaced a revision they do not supersede (`forked`, each
  an `archive::Fork` with the displaced and current revisions' uuids), for
  a combine that must choose or rename.
- `Archive::schema_version`, and `Error::SchemaTooOld`, returned for a
  document written into a v4 archive.

### Changed

- **Breaking (format):** the schema version is 5 (was 4). Every archive this
  release creates has the `documents` table and is stamped 5; this release
  reads 4 and 5. A v4 archive reads as holding no documents, and is still
  written in place as v4 (a writer resumes it, metadata edits and compaction
  run), but no document is written into it: copy it forward with
  `rewrite::copy_sources_into` into a new archive, which is v5. The reason
  is in FORMAT.md §8: 0.5.1's copier drops a table it does not know, and
  documents are data people wrote.
- **Breaking (format):** on schema 5 the caller's per-segment index is a
  row of a new `segment_indexes` table, keyed like its segment, instead of
  the `segments.caller_index` column (FORMAT.md §3.2). SQLite stores a
  row's columns in order and spills a large BLOB onto overflow pages, so an
  index after the segment bytes was reachable only through them: reading
  every index of a cold archive cost as much I/O as reading the segments.
  `Archive::read_segment_indexes` now reads no segment row on v5. The table
  cascades on segment delete, so retention, compaction and
  `Transaction::delete_segment` remove an index with its segment. A v4
  archive keeps its indexes in the column, is written there when written in
  place, and is read through the same API; a copy into a new archive moves
  them to the table. The Rust API is unchanged: `SegmentRow::caller_index`
  and `insert_segment_with_index` carry the index as before.
- **Breaking:** `SegmentEncoder::encode` and `encode_after` take
  `&segment::EncodeContext` in place of the stream name, and
  `segment::materialize` takes one too. The context carries the stream
  (`ctx.stream`), the source's row id in the archive (`ctx.source_id`) and
  its uuid (`ctx.source_uuid`). Before, an encoder was told only the stream
  name, which one writer with several sources (two hosts recording `cpu`)
  repeats, so an encoder keeping per-stream state could not key it. The
  writer, a reader materializing a tail and a copy all pass the source the
  rows are in; a copy passes the source archive's, not the destination's.
  The struct is `#[non_exhaustive]`, so fields are added without a break.
  For an implementor: replace `stream: &str` with `ctx: &EncodeContext<'_>`
  and read `ctx.stream`; calls an implementation makes to its own `encode`
  or `encode_after` (an `encode_after` that falls back to `encode`, say)
  pass `ctx` instead of the stream too. metriken-storage needed about eight
  lines in three files. A caller driving an encoder itself (a test) builds
  one with `EncodeContext::new(source_id, stream)`. Key state that outlives
  one archive by `source_uuid`: a copy renumbers `source_id`.
- `rewrite::copy_sources_into` also copies the source archive's documents,
  every revision, whatever the `CopySpec` selects. Called once per input,
  as a combine does, it carries each input's documents; call
  `copy_documents_into` first for the report.

## [0.5.1] - 2026-10-19

### Added

- `ArchiveMut::update_source_labels`, which replaces one source's labels in
  place, as `update_source_metadata` replaces its metadata. The source keeps
  its `uuid`. An assembly of several archives uses it to tag each copied
  source (an A/B arm) without rewriting segments.

## [0.5.0] - 2026-10-14

### Changed

- **Breaking:** arrow and parquet are 60 (were 58). Parquet 59 and later no
  longer depend on `thrift`, which clears GHSA-2f9f-gq7v-9h6m (thrift 0.17).
  Arrow and parquet types appear in the public API, so a caller must use the
  same major version: `segment::encode_batch` takes an arrow `Schema` and
  `RecordBatch`, `segment::writer_props` returns a parquet
  `WriterProperties`, `CompactSpec::writer_props` and
  `CopySpec::writer_props` hold one, `MergeInput` carries parquet
  `KeyValue`s and arrow `Fields`, `MetadataMerge::finish` returns
  `KeyValue`s, and `ColumnFilter`'s methods take an arrow `Field`.
- **Breaking:** the minimum supported Rust version is 1.88 (was 1.85), the
  floor arrow and parquet 60 set. Parquet 59 would clear the advisory at
  1.85; 60 matches the metriken crates, which take arrow and parquet types
  across this API and move to 60 with it.

## [0.4.2] - 2026-10-13

### Added

- `SourceWriter::seal_capped(batch, max_rows)`: a seal that writes no
  segment with more than `max_rows` WAL rows. A stream with more live rows
  than that is sealed as consecutive segments of `max_rows` rows, oldest
  first, and a last one with the remainder (7 live rows at a cap of 2 are
  segments of 2, 2, 2 and 1), each with its own catalog entry and `seq`,
  all in the batch's one transaction. The cap counts WAL rows, as the
  catalog's `rows` does, not the parquet rows an encoder writes for them.
  Only the row cap is applied: the byte cap and the age bound remain
  triggers for when the caller seals, because the writer does not have the
  byte estimate the caller's `SegmentAccount` charged per row. A seal that
  is deferred (its transaction could not commit) leaves its rows live, and
  the next capped seal of the stream splits them with the rest.
- `SegmentEncoder::encode_after(stream, before, rows)`: how a capped seal
  encodes each part after the first, with the stream's earlier live rows
  as `before`. The default ignores `before` and calls `encode`, which is
  right when every row decodes on its own. An encoder whose rows depend on
  earlier ones (a schema carried only by the first row after a seal) must
  implement it before its caller uses `seal_capped`. A part whose segment
  drops a row at an edge it shares with another part, or that yields no
  segment, abandons the split for that stream, which is then sealed as one
  segment as `seal` would, so a split never loses a row. That holds only
  for an encoder whose segment reports the rows it wrote: one that claims
  rows it skipped hides the gap from this check.
- `Evicted::held_streams`: how many streams a writer retention pass left
  out because their seal was deferred and still could not commit.

### Fixed

- A writer retention pass (`SourceWriter::evict_before` and its variants)
  deleted the live rows of a stream whose seal had been deferred, the same
  as any other live row past the cutoff. For an encoder whose rows depend
  on an earlier one (a schema stated once at a segment's start), that cut
  the row the survivors needed, and the next seal decoded none of them.
  The writer now remembers which streams' seals deferred, seals them at the
  start of the next pass (retried like any seal), and leaves out of the
  pass any stream it still cannot seal; that stream is evicted by the
  first pass after its seal commits. The stream is left out whole, its
  sealed segments and caller rows included. While a stream is held, a
  whole-source pass runs stream by stream, so a `CallerRowFloor` is asked
  for every stream name rather than only the names with caller rows. A
  pass after a deferral first retries the deferred streams' seals, each in
  its own transaction, on the writer thread; after the first one that
  still cannot commit (one retry schedule: up to four busy timeouts and
  310 ms) the rest are held without being tried. A retried seal that
  fails with an error that is not retryable, such as an encoder error, is
  not held: it is logged, the pass evicts the
  stream's live rows as it did before, and a `Seal` of the stream that
  fails the same way stops the writer, as it always has.
- A seal put every live row of a stream into one segment, so a caller that
  checked `SegmentAccount::is_due` once after several rows had arrived
  (frames that queued while a slow seal ran) got segments past its
  policy's `max_rows`: 3- and 4-row segments against a cap of 2 at 10 Hz
  with ~350 ms seals. `seal_capped` with the policy's `max_rows` bounds
  them. `seal` is unchanged.

## [0.4.1] - 2026-10-12

### Added

- `rewrite::compact_with` and `rewrite::compact_stream_with`: compaction
  with a caller-supplied `rewrite::MetadataMerge` that decides the merged
  segment's file key-value metadata. It is a fold over the run's segments in
  order, each given once as a `rewrite::MergeInput` (its metadata, columns
  and row count): `start` with the first, `extend` with each next one, which
  may refuse it and so end the run before it, and `finish` for the segments
  merged. A segment the `SchemaPolicy` refuses ends the run before the merge
  is given it. A merge that keeps what it has decoded in its `Run` costs time
  linear in the run. `compact` and `compact_stream` use
  `rewrite::IdenticalMetadata`, which merges only segments whose metadata has
  the same entries; its `Run` is `rewrite::IdenticalMetadataRun`. A caller
  whose metadata changes at every segment, such as a list of the occupants
  the rows refer to, which changes whenever the population does, can merge
  across those changes by combining the lists.
  `MergeInput::new` builds an input by hand, for testing a merge without an
  archive; `MergeInput::rows` is the segment's parquet row count, which for a
  stream that writes several parquet rows per WAL row is larger than the
  catalog's `SegmentMeta::rows`.
- `rewrite::compaction_opens` (`test-support`): how many segments compaction
  has opened on the calling thread.

### Fixed

- Compaction (`rewrite::compact`, `rewrite::compact_stream`) cataloged a
  merged segment's `rows` as its parquet row count. Everywhere else the
  catalog's `rows` counts the WAL rows a segment covers (the seal path
  writes it, and `segment::materialize` refuses a segment whose count
  differs), and the two differ for an encoder that writes several parquet
  rows per WAL row, such as a long layout with one parquet row per occupant
  per WAL row. On one long recording, 0.4.0's `compact` took a
  `cpu_usage_task/occupants` stream from 3,383 catalog rows to 264,286, and
  with `CompactSpec::unioning_fields` it also took `cpu_usage_task` from
  34,678 to 76,182,021 and metriken's `ArchiveReader::interval()` from 1 s to
  0.001 s; this release's caller merge would have done the same to
  `cpu_usage_task` without the fix. A merged segment now catalogs the sum of
  its inputs' `rows`. Segments already compacted keep the inflated count, and
  the compaction planner, which sizes runs by that count, treats each as over
  any target below it, so they are not merged again.
- Compaction (`rewrite::compact`, `rewrite::compact_stream`) did not carry
  the parquet file key-value metadata of the segments it merged into the
  merged segment's file metadata. Under `StopAtChange` it moved the entries
  into the merged file's embedded `ARROW:schema`, and under `UnionFields` it
  dropped them entirely, so a caller that describes a segment there (a
  layout marker, a per-column list of what the rows refer to) got a merged
  segment that read as a different kind of segment. The merged segment now
  carries its inputs' metadata, Arrow's schema entry aside, with an input's
  value replacing the writer properties' for the same key. A run stops
  where any input's metadata has different entries from the first's,
  compared without regard to their order, under either `SchemaPolicy`;
  before, only `StopAtChange` stopped there, because it compares whole
  Arrow schemas, and `UnionFields` merged across the change. Segments
  already compacted are not repaired.
- `rewrite::project_segment_columns` wrote a key twice when the writer
  properties and the segment both carried it. The segment's value now
  replaces the properties', as in compaction.

## [0.4.0] - 2026-10-11

### Changed

- **Breaking:** a feature `archive`, on by default, gates the container and
  everything that reads or writes it: `archive`'s SQLite part, `read`,
  `rewrite`, `segment`, `replicate::ArchivePublisher`, the `Error::Sqlite`
  variant, `Error::sqlite`, `Error::sqlite_code` and
  `From<rusqlite::Error> for Error`, and the arrow, bytes, parquet, rusqlite,
  serde_json and tracing dependencies. `write` implies it. With no features
  the crate is the replication wire (`replicate::{Frame, wire, IndexKind,
  IndexState, NO_INDEX_STATE}`, `keys`, `archive::{WalRow, SegmentMeta}`,
  `seal`, `Error`) and has no dependencies. A user that sets
  `default-features = false` and reads archives adds
  `features = ["archive"]`. `Error::is_retryable` and `Error::is_constraint`
  are false without it.

### Fixed

- The docs.rs build. It failed for 0.3.3 and 0.3.4 because nightly removed
  `feature(doc_auto_cfg)`; the crate now enables `feature(doc_cfg)`.

## [0.3.4] - 2026-10-02

### Added

- A writer heartbeat. `sources` gains two nullable columns, `heartbeat` and
  `heartbeat_interval_ns` (no schema version change, FORMAT.md §3.1 and §8).
  The writer sets the interval when it adds or resumes a source and bumps
  the heartbeat of every source it has open every `HEARTBEAT_INTERVAL`
  (5 s), whether or not it has rows to commit, until the source is
  finalized. A writer that reopens an older archive adds the columns.
  Copies carry both values unchanged.
- `SourceRow::heartbeat` and `SourceRow::heartbeat_interval_ns`, and
  `archive::HeartbeatWatch`, which turns successive reads of a source into a
  `WriterState`: `Complete`, `Live`, `Stopped` (the heartbeat has not
  changed for `STOPPED_AFTER_INTERVALS` = 3 intervals: a killed writer or a
  copy) or `Unknown` (no heartbeat).
- `writer::HEARTBEAT_INTERVAL`, `archive::STOPPED_AFTER_INTERVALS`,
  `Transaction::set_heartbeat`, `Transaction::set_heartbeat_interval`,
  `Transaction::beat` and `ArchiveMut::ensure_heartbeat_columns`.
  `WriterState` is `#[non_exhaustive]`. A resumed source continues its
  heartbeat count; an interval of zero or less reads as `Unknown`.
- `Writer::create_beating_every` (feature `test-support`) chooses the
  heartbeat interval.

## [0.3.3] - 2026-09-29

### Added

- `ColumnFilter::projects(stream)`, default `true`: a stream it returns
  `false` for is copied as it is by a `keep_columns` pass, sealed segments
  byte for byte and its tail unprojected. For a companion stream whose
  columns a field-by-field filter cannot tell from the data's.

### Fixed

- `project_segment_columns` keeps the source file's key-value metadata
  (all but `ARROW:schema`, which the writer writes for the projected
  schema). It wrote the projection with only the caller's properties, so a
  segment marked by its caller (a layout, a list of what its rows refer to)
  lost the mark and could read back as another kind of segment.

## [0.3.2] - 2026-09-28

### Fixed

- `Archive::vacuum_into` works on the read handle. `Archive::open` sets
  `query_only`, and SQLite refused `VACUUM INTO` under it ("attempt to write
  a readonly database") although the statement writes only the
  destination, so a live archive, which allows a second party no other
  handle, could not be dumped this way. `query_only` is lifted for the one
  statement and restored on every path; the connection is still
  `SQLITE_OPEN_READ_ONLY`, so the archive stays unwritable.

## [0.3.1] - 2026-09-28

### Added

- `Archive::last_caller_row_at_or_before(source_id, stream, upto, pred)`:
  the newest caller row at or before `upto` that `pred` accepts, walking
  back and decoding rows only until one matches. A reader of a log that
  restates its state periodically finds the restatement to replay from
  without reading the log from its start.

## [0.3.0] - 2026-09-27

### Changed

- **Breaking:** `Frame` is `#[non_exhaustive]`. A `match` on it needs a
  wildcard arm; adding a frame kind is then not a breaking change.
- `FrameReader::next_frame` skips a frame of a kind it does not know and counts
  it in the new `FrameReader::skipped`, as `WIRE.md` §7 already said it did. It
  returned an error, so a subscriber older than its publisher stopped at the
  first new kind rather than losing only what that kind carried.
  `decode_payload` on a single payload still refuses an unknown kind.

### Fixed

- `verify` returned an error when SQLite's integrity check ran into the damage
  it was walking and failed partway with `SQLITE_CORRUPT`. That is the check
  finding corruption, and it is now reported as `Problem::Corrupt` like any
  other line. Damage to page 2, the first pointer-map page, reproduced it on
  every archive; a new test sweeps a damaged page across the file.

## [0.2.2] - 2026-09-25

### Added

- **`caller_rows` floors on retention.** `ArchiveMut::evict_before_with_floor`
  and `evict_streams_before_with_floor`, and the `SourceWriter` methods of the
  same names, take a floor function (`CallerRowFloor`, boxed as
  `writer::CallerRowFloorFn`). The pass calls it once per name it touches,
  inside its transaction and after the segment and WAL deletes, with the
  oldest row that name still holds, and deletes the name's caller rows only
  below `min(floor, cutoff)`. The existing eviction methods are unchanged.

  Retention cut a stream's `caller_rows` at the segment cutoff, in the same
  transaction as the segments, with no way for the caller to keep more. For an
  identity log of `Full` and `Delta` entries, a cutoff between a `Full` and
  the deltas after it deleted the `Full` while the deltas and the rows they
  describe remained, so a reader could not attribute those rows. The caller
  knows which entry is a `Full` and dendro does not read the blobs, so the
  caller supplies the floor: the latest `Full` at or before the oldest row the
  stream still holds, or `i64::MIN` for a stream with none. The oldest row
  rather than the cutoff, because a segment spanning the cutoff is kept with
  rows older than it; and asked inside the pass, because a seal between a
  caller's own query and the pass can create such a segment.

- `SourceWriter::evict_before` was documented as fire-and-forget. It waits for
  the pass and returns its result; the doc now says so.

### Changed

- `Frame::Rows.seq` **counts intervals, not frames**, and a gap means intervals
  the subscriber did not receive — *not* "frames lost in transit and nothing
  else", which 0.2.1 said and which no publisher that declines to buffer can
  hold.

  Rule 6 assumed a publisher can always emit. One that refuses to buffer without
  bound for a stalled consumer cannot, and buffering without bound is the worse
  failure. So an interval may legitimately go unserved, and the contract now has
  three states rather than two: served with data, served with nothing (the empty
  frame), and not served (a gap).

  This does **not** reinstate the interval index. A per-interval counter kept by
  the producer's own loop needs no clock and does not alias; dividing an
  observation timestamp still does.

- The `write` feature's description said `std::thread::spawn` "panics at
  runtime" on wasm32. This crate uses
  `std::thread::Builder::spawn`, which returns `io::Result` and is reported by
  name, so the failure is diagnosable rather than a trap. Also records why a
  compile check cannot verify the reader configuration's thread-freedom.


## [0.2.1] - 2026-09-18

### Changed

- `Frame::Rows.seq` is a **plain sequence number** — strictly increasing,
  contiguous, one per frame, not a timestamp — and a gap means **frames lost in
  transit**. This reverses guidance added earlier in this section, which
  recommended an interval index; that recommendation was wrong and is removed.

  It contradicted rule 6, in the same sentence. Rule 6 already requires a frame
  per interval, empty when nothing was observed, so there is no skipped interval
  for a counter to miss — the empty frame is how "nothing was observed" is
  stated. An interval index merges that fact with "a frame did not arrive",
  losing the distinction, and aliases on sub-interval jitter: a reading taken
  slightly early lands in the previous bucket, which invents gaps that did not
  happen and hides gaps that did.

  What survives from that guidance is that `seq` **need not start at zero**,
  which the code never required.

## [0.2.0] - 2026-09-18

**This is 0.2.0, not 0.1.1.** The root re-exports below are removed public
items, which Cargo's SemVer reference calls a major change, and for a `0.x`
crate the minor is the major position. `0.1.1-alpha.0` was the automated
post-release bump (`chore: begin next development iteration`) rather than a
decision, and shipping under it would break `dendro = "0.1"` callers on a
`cargo update`. The replication work on its own would have been a legitimate
`0.1.1`.

**The crate version and the schema version answer different questions.** The
archive schema version is unchanged at **4** -- nothing on disk moves, existing
archives are unaffected, and `FORMAT.md` section 8 governs that surface alone.
The Rust API broke separately. Neither implies the other.

### Added

- `replicate::wire::LENGTH_PREFIX_BYTES`, the offset at which a frame's payload
  begins. `encode` returns a whole frame and `decode_payload` takes the payload
  alone, because `FrameReader` has already consumed the prefix; a caller pairing
  the two by hand needed a bare `4`.

- **Replication**: frame types and a wire codec,
  `Subscriber` (apply frames to an archive) and `ArchivePublisher` (tail one).
  Transport is not included. One frame kind per table that `rewrite` carries, so
  the set has a completeness check rather than a guess; a live tail ships rows
  and the subscriber seals its own, while a catch-up ships sealed segments. An
  index entry stays opaque — the state hash a subscriber needs travels outside
  the blob. The wire format is specified in `WIRE.md`. It is not behind a
  feature — it adds no dependency and builds wherever the crate does, including
  wasm32. `Subscriber` alone needs `write`, because it drives the writer's
  thread; publishing is a read.
- `Writer::add_source_with_uuid`, which carries an identity minted elsewhere so
  a copy **is** the source rather than another one with the same labels.
- `SourceWriter::adopt_segment`, which inserts a segment built elsewhere at a
  stream's next `seq`. Refuses one that straddles the stream's watermark or
  reaches an unsealed row, and reports one the watermark already covers as
  already held rather than as a failure.
- `SourceWriter::clock_offset` and `ArchiveMut::insert_source_with_uuid`, for
  carrying a clock-drift series and an identity that were observed elsewhere.


### Removed

- **Breaking:** the crate-root re-exports of `Archive`, `ArchiveMut`,
  `Transaction`, `Writer` and `SourceWriter`. Spell them by their module —
  `dendro::archive::Archive`, `dendro::writer::Writer` — which is what this
  crate's own tests, doc examples and intra-doc links already did; nothing
  inside dendro used the root path for any of them. `Error`, `Result` and
  `ReadOnly` stay at the root, which is what callers actually reach for.

  The rule is now statable: the error type and `Result` live at the crate root,
  every other type lives in its module. `replicate` already followed it and
  stops being an exception.

### Changed

- `SegmentEncoder::version` documents why it is a borrowed string: the value
  lands in `sources.metadata`, which the format defines as string to string, and
  dendro compares it for equality only. No signature change.

## [0.1.0] - 2026-09-16

First release. The crate is a segmented Parquet archive with a write-ahead
log, in a single SQLite file: rows land in the WAL and are periodically sealed
into immutable parquet segments, with the catalog, retention, crash recovery
and rewriting around them. A row is a timestamp, a wall-clock offset and opaque bytes; what the
bytes *mean* is the caller's, expressed through one trait.

### Added

- Writer, source, stream, segment and row model, with the WAL readable rather
  than a staging area, and snapshot isolation for readers.
- `SegmentEncoder`, the whole schema boundary, plus an encoder version marker
  that refuses a reader whose encoder disagrees with the writer's.
- Per-stream retention by time, with the size accounting a cap needs.
- Reopening an archive and resuming a source, with writer sessions recorded
  and a floor that refuses an append behind where the previous session stopped.
- Out-of-order appends are dropped rather than silently stored, counted per
  source and logged once per stream.
- The caller's time-keyed store, `caller_rows`: opaque rows against
  `(source, stream, ts)` that the archive reads by range, evicts by the same
  cutoff as segments, carries verbatim in every copy, and never decodes or
  merges. `Transaction::insert_caller_rows`, `ArchiveMut::insert_caller_rows`,
  `SourceWriter::caller_rows`, `Archive::read_caller_rows` and
  `Archive::caller_row_streams`; `Evicted::caller_rows` counts what
  retention removed. A store row does not make a stream exist. This is the
  store the schema-churn journal entry owed to a caller that keeps column
  identity in a secondary index.
- An opaque per-segment index slot, so a caller that builds its own index over
  segment contents can keep it in the archive and stay one file.
- `Archive::verify`, reporting every problem it finds in one pass.
- `read::describe`, one call for the whole catalog plus the file's own size.
- Optional wall-clock alignment for segment boundaries.
- `rewrite::compact`, merging a stream's small segments and reclaiming the
  space, measured at 18.2x read and 2.38x size between 400 segments and one.
- `SchemaPolicy::UnionFields`, opting into merging across a column set that
  grew or shrank.
- Rewriting: combine, trim, range-copy and opt-in column projection.
- An archive is exactly a file that carries dendro's header stamp. There is
  no fallback to a catalog table and no legacy schema: a `.rez` recording
  from before dendro is upgraded by rezolus, which still reads it.
- `archive::ArchiveMut`, the write handle, beside `Archive`, which now only reads.
  `Archive::open` is read-only (the former `open_read_only`), works on read-only
  media and on a file another process is writing, and is the only open the
  read paths use. `ArchiveMut::create` makes a new archive; `ArchiveMut::open` takes an
  existing one with SQLite's exclusive locking mode, so it is refused as
  `Error::InUse` while anything else holds the file, and it refuses read-only
  media by name. `ArchiveMut` derefs to `Archive`. Before this, a
  second read-write connection on a file the writer thread held wrote
  silently and left the writer's cached sequence numbers and watermarks
  wrong. Seven crate-internal methods (`open_for_write`, `remove_archive`,
  `next_seqs`, `source_time_span`, `total_wal_rows`, the pragma readers) left
  the public surface, and `ReadOnly::Handle` is gone because a read handle
  has nothing to refuse.
- Each `writer_sessions` entry records the dendro crate version that
  appended, under `dendro`. Provenance for tracing a defect to the sessions
  that had it, never a gate: readability is the header's schema version.
- `keys::PRODUCER_VERSION`, a reserved metadata key for the version of the
  software that produced a source's values. Written by the producer, stored
  opaquely, never parsed. Distinct from `encoder`, which versions the row
  *encoding* and which dendro enforces: a producer that keeps its encoding and
  changes what it measures moves every value while the encoder version stays
  identical, which is the case `encoder` cannot see.
  (rezolus [#1195](https://github.com/iopsystems/rezolus/issues/1195).)

### Notes

- **Minimum supported Rust version is 1.85**, the floor the dependency graph
  sets. Raising it is a minor-version change and is gated in CI.
- The reader configuration (`--no-default-features`) drops the writer thread
  and is what builds for `wasm32-unknown-unknown`. It is a supported
  configuration, tested in CI.

## Before 1.0

These are release commitments tracked here rather than in the journal:

- **`#[non_exhaustive]` on the caller-constructed types.** The types dendro
  returns carry it already. The specification types — `SealPolicy`,
  `CopySpec`, `CompactSpec` — and the model types `Segment`, `WalRow`,
  `SourceMeta` and `SegmentMeta` do not, because marking them requires
  constructors and breaks every struct literal downstream. Each of those
  specification types has already gained a field at least once, so this is
  the one most likely to break a downstream build.
- **`wall_offset` and `clock_offsets` may move.** A row carries a wall-clock
  offset, and the archive derives a per-source `clock_offsets` series from
  it. Both are a telemetry concept living in the container, as the
  encoder-boundary journal entry records. They stay in 0.x because the one
  caller uses them, and they may move into
  the encoder's payload or the source's metadata before 1.0. Build on the
  timestamp; treat the offset as provisional.
- **The encoder boundary has one caller.** Recorded as an open journal entry
  rather than a defect: the generality claim is not yet earned by a second
  implementation.

Done since this list was written: every public field is documented
(`#![warn(missing_docs)]` with warnings denied in CI), and `FORMAT.md` §8
states the compatibility policy.
