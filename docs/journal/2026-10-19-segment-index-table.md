---
status: open
opened: 2026-10-19
updated: 2026-10-19
---

# Segment indexes out of the segment row, and the encoder told its source

## Goal

Make a caller's per-segment index cheap to read on a cold archive, and give
an encoder enough context to key per-stream state when one writer holds
several sources. Both ship in 0.6.0 with schema 5 (owner decision,
2026-10-19), because both change something a release has to change anyway:
the schema, and the `SegmentEncoder` trait.

## Why

A prototype in metriken (`proto/segment-index`, `7f67b39`) stores a
three-tier catalog index per segment in `caller_index`: metric names and
kinds, label keys per metric, and distinct values per key up to 64, zstd,
about 332 bytes a segment. Warm, it removes every segment fetch from
opening an archive. Cold, it gained little: at N=64 sources the open took
2977 ms without the index and 2095 to 2239 ms with it. The cause is the row
layout. `caller_index` was the last column of `segments`, after `bytes`, and
SQLite stores a row's columns in order, spilling a large BLOB onto overflow
pages; reaching a column after the BLOB walks those pages. On the 9.6-hour
archive, reading every `caller_index` cold read 110 MB in 470 to 620 ms,
against 1.3 MB and 10 ms for `seq` alone, and a 12-byte index cost the same
as a large one.

Separately, `SegmentEncoder::encode` was told only the stream name. One
writer can hold several sources with the same stream names (two hosts
recording `cpu`), so an encoder that keeps per-stream state (a long table's
occupant labels stated once, an index built across seals) could not key it.

## Design

- **`segment_indexes(source_id, stream, seq, bytes)`, `WITHOUT ROWID`,**
  keyed like its segment, with a cascading foreign key to `segments`
  (FORMAT.md §3.2). A separate table rather than a column placed before
  `bytes`: a column's position is fixed when the table is created, so
  reordering `segments` means rebuilding it anyway, and a separate table
  also lets a segment with no index have no row. `WITHOUT ROWID` stores a
  stream's indexes contiguously in key order, so a cold read of one stream's
  indexes is a range of leaf pages. The read
  (`Archive::read_segment_indexes`) is answered from the `segments`
  primary-key index and this table and reads no segment row; a unit test
  pins the query plan (`reading_v5_indexes_touches_no_segment_row`).
- **The cascade is the delete path.** Retention, compaction and
  `Transaction::delete_segment` all delete from `segments`, and each writer
  connection runs with `foreign_keys=ON`, so the index goes with its segment
  without each path having to remember it. `verify` reports an orphan
  through `foreign_key_check`.
- **v4 stays v4.** The archive's layout (`IndexLayout`: table, column, or
  absent for a v4 file from before the column) is read once at open. A v4
  archive written in place keeps its indexes in the column; a copy into a
  new archive moves them to the table. The Rust API did not change:
  `SegmentRow::caller_index` and `insert_segment_with_index` carry the
  index on both layouts.
- **`EncodeContext`** replaces the stream argument of `encode` and
  `encode_after`: the stream, the source's row id and its uuid,
  `#[non_exhaustive]` with `EncodeContext::new` for callers that drive an
  encoder themselves. The writer, a reader materializing a tail, and a copy
  all pass the source the rows are in; a copy passes the source archive's,
  not the destination's (tested: `tests/encode_context.rs`).

## Measurement

Pending: cold and warm open with the metriken prototype adapted to the
table, on the 9.6-hour archive and the N=1/4/16/64 bench archives, by the
same cold-cache method as the prototype's (`msync(MS_INVALIDATE)` and a
`mincore` check).

## Reopen

- If a caller's index grows past a page or so per segment, a cold read of
  every index is again proportional to their total size; the table only
  removes the segment BLOBs from the read.
