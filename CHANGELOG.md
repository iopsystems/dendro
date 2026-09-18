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
