# Changelog

Notable changes per release. This file follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this crate follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html), with the caveat
that while the major version is 0 **any release may break**, as permitted by
Cargo's convention; see *Before 1.0* below.

The reasoning behind a change lives in [`docs/journal/`](docs/journal/README.md),
one entry per effort. This file says what changed; the journal says why, and
carries the measurements.

## [Unreleased]

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
- Legacy-schema archives open read-only through compatibility views.
- `archive::ArchiveMut`, the write handle, beside `Archive`, which now only reads.
  `Archive::open` is read-only (the former `open_read_only`), works on read-only
  media and on a file another process is writing, and is the only open the
  read paths use. `ArchiveMut::create` makes a new archive; `ArchiveMut::open` takes an
  existing one with SQLite's exclusive locking mode, so it is refused as
  `Error::InUse` while anything else holds the file, and it refuses read-only
  media and legacy archives by name. `ArchiveMut` derefs to `Archive`. Before this, a
  second read-write connection on a file the writer thread held wrote
  silently and left the writer's cached sequence numbers and watermarks
  wrong. Seven crate-internal methods (`open_for_write`, `remove_archive`,
  `next_seqs`, `source_time_span`, `total_wal_rows`, the pragma readers) left
  the public surface, and `ReadOnly::Handle` is gone because a read handle
  has nothing to refuse.
- `keys::PRODUCER_VERSION`, a reserved metadata key for the version of the
  software that produced a source's values. Written by the producer, stored
  opaquely, never parsed. Distinct from `encoder`, which versions the row
  *encoding* and which dendro enforces: a producer that keeps its encoding and
  changes what it measures moves every value while the encoder version stays
  identical, which is the case `encoder` cannot see.
  (rezolus [#1195](https://github.com/iopsystems/rezolus/issues/1195).)

### Fixed

- `FORMAT.md` omitted `encoder` from the reserved-keys table and its
  compatibility section still described the key as a convention nobody had
  built. It is built and enforced; both now say so.
- The read-only open failed on read-only media, where every document said
  it was the open to use. WAL mode must create the `-shm` sidecar, which
  read-only media refuses. It now retries with SQLite's `immutable=1` when
  that happens and no `-wal` sidecar exists, and refuses rather than reads
  short when one does. `tests/read_only_media.rs`.
- The documents said an unclean kill loses at most one append. The writer's
  channel holds one tick while another is mid-commit, so the bound is two
  ticks, plus whatever the caller has staged. Stated as such.
- The documents described a killed archive as a 4 KiB file with no tables.
  Creation has checkpointed the catalog into the archive since the header
  stamp landed, so the archive alone always opens; the numbers are
  re-measured (45 KiB archive, 3.3 MB sidecar, 61 KiB after a read-write
  open folds it in).
- Two different compaction measurements (the seal-coarse arm at 18.2x and
  2.38x, the compacted result at 18.6x and 2.37x) were cited as one number.
  Each site now says which it cites.
- The 3.14x per-tick write amplification that decided the page size was
  cited by a test and present in no document. Restored to `DESIGN.md` with
  the sweep it came from.

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
  caller uses them and the legacy reader needs them, and they may move into
  the encoder's payload or the source's metadata before 1.0. Build on the
  timestamp; treat the offset as provisional.
- **The encoder boundary has one caller.** Recorded as an open journal entry
  rather than a defect: the generality claim is not yet earned by a second
  implementation.

Done since this list was written: every public field is documented
(`#![warn(missing_docs)]` with warnings denied in CI), and `FORMAT.md` §8
states the compatibility policy.
