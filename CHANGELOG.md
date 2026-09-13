# Changelog

Notable changes per release. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this crate follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html), with the caveat
that while the major version is 0 **any release may break**, which is the
Cargo convention and is used deliberately here — see *Before 1.0* below.

The reasoning behind a change lives in [`docs/journal/`](docs/journal/README.md),
one entry per effort. This file says what changed; the journal says why, and
carries the measurements.

## [Unreleased]

First release. The crate is a segmented-parquet archive with a write-ahead
log, in a single SQLite file: rows land in the WAL and are periodically sealed
into immutable parquet segments, with the catalog, retention, crash recovery
and rewriting around them. A row is opaque bytes and a timestamp; what a row
*means* is the caller's, expressed through one trait.

### Added

- Archive, source, stream, segment and row model, with the WAL readable rather
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
- `Db::verify`, reporting every problem it finds rather than the first.
- `read::describe`, one call for the whole catalog plus the file's own size.
- Optional wall-clock alignment for segment boundaries.
- `rewrite::compact`, merging a stream's small segments and reclaiming the
  space, measured at 18.2x read and 2.38x size between 400 segments and one.
- `SchemaPolicy::UnionFields`, opting into merging across a column set that
  grew or shrank.
- Rewriting: combine, trim, range-copy and opt-in column projection.
- Legacy-schema archives open read-only through compatibility views.

### Notes

- **Minimum supported Rust version is 1.85**, the floor the dependency graph
  sets. Raising it is a minor-version change and is gated in CI.
- The reader configuration (`--no-default-features`) drops the writer thread
  and is what builds for `wasm32-unknown-unknown`. It is a supported
  configuration, tested in CI, not merely a wasm artifact.

## Before 1.0

Commitments to make while breaking is still free. Tracked here rather than in
the journal because they are release mechanics rather than efforts:

- **`#[non_exhaustive]` on the caller-constructed types.** The types dendro
  returns carry it already. The specification types — `SealPolicy`,
  `CopySpec`, `CompactSpec` — and the model types `Segment`, `WalRow`,
  `SourceMeta` and `SegmentMeta` do not, because marking them requires
  constructors and breaks every struct literal downstream. Each of those
  specification types has already gained a field at least once, so this is
  the one that will bite.
- **A stated compatibility policy** for the schema version, the legacy schema,
  and the reserved metadata keys. `FORMAT.md` specifies the layout precisely
  and says nothing about what a version bump promises an existing reader, or
  whether legacy support is permanent.
- **`#![warn(missing_docs)]`.** Every public type is documented; roughly
  eighty public *fields* and enum-variant fields are not. Worth closing before
  the docs are what people build against.
- **The encoder boundary has one caller.** Recorded as an open journal entry
  rather than a defect: the generality claim is not yet earned by a second
  implementation.
