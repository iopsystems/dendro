---
status: implemented
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

The metriken prototype rebuilt against this branch (`proto/segment-index`
`166bd59`, a path dependency and the `EncodeContext` signature, nothing
else), with the prototype's bench (`index-bench`, release, `debug = 1`).
Archives as before: a 30-minute slice of `metrics-9.6h.dendro` (352
segments, 49 streams, 75.5 MB; the original N=1 file was gone, and the
rebuilt one matches it in size and counts), copied into 1, 4, 16 and 64
sources, and the 9.6-hour archive itself; each rewritten with no catalog
index (`fp`, the 12-byte name fingerprint) and with the three-tier index
(`a-16`, `a-64`, `b-64`). Index bytes per segment came out identical to
the earlier run (12.0, 159.9, 333.1, 124.1). Cold means the file's pages
were dropped (`msync(MS_INVALIDATE)` on a shared mapping) and `mincore`
showed 0 resident before the open. Warm 5 reps, cold 3; medians, with the
range. Apple M-series laptop, macOS, one session. "Before" is the earlier
run's log (`tmp/index/logs/open.txt`), index in the `caller_index` column;
"now" is the table.

Reading every index cold (`idxread`):

| Archive | before: `caller_index` column | now: `segment_indexes` | now, through `read_segment_indexes` |
| --- | --- | --- | --- |
| 9.6 h, `a-64` (1.9 MB of index) | 498 ms, 111 MB resident | 60 ms, 9.1 MB | 78 ms, 11.6 MB |
| 9.6 h, `fp` (70 KB) | 622 ms, 110 MB | 11 ms, 1.6 MB | 32 ms, 4.4 MB |
| N=64, `a-64` (7.5 MB) | 2798 ms, 432 MB | 227 ms, 35 MB | 346 ms, 47 MB |
| N=64, `fp` (270 KB) | 2250 ms, 424 MB | 43 ms, 6.2 MB | 156 ms, 18 MB |

The keys alone (`segments.seq`) were 9 to 11 ms on the 9.6-hour archive,
with one 25 ms outlier, and 35 to 42 ms at N=64 in both runs, so that is
the floor. A read of the table now scales with the index bytes rather than
with the segment bytes.

Caveats on that table: each cell is one cold run. `idxread` evicts the file
before each read but does not check resident pages before it, as the open
bench does with `mincore`. The `read_segment_indexes` timer starts after
`Archive::open`, so it leaves out the open itself.

Open, cold, index on (catalog served from the index, no segment fetched):

| Archive | before (column) | now (table) |
| --- | --- | --- |
| N=1 `a-64` | 31.0 ms [31-32] | 24.0 ms [23-24] |
| N=4 `a-64` | 176.9 [171-199] | 95.9 [90-96] |
| N=16 `a-64` | 628.4 [566-735] | 451.3 [434-493] |
| N=64 `a-16` | 2178 [2140-2850] | 1473 [1470-1477] |
| N=64 `a-64` | 2239 [2184-4007] | 1538 [1521-1580] |
| N=64 `b-64` | 2095 [2001-3086] | 1391 [1356-1399] |
| 9.6 h `a-64` | (not run) | 351 [349-351] |
| 9.6 h `b-64` | (not run) | 304 [302-308] |

Resident after a cold N=64 open went from 427-432 MB to 244-257 MB.

The no-index baseline moved too: N=64 `fp` cold, index off, 2977 ms before
and 2476 ms now; warm, 1157 and 722. The prototype reads every segment's
index on open even with the catalog index off (`index_rows=22528` in those
lines), so the old baseline included the overflow walk for a 12-byte
fingerprint. That is an inference from the log, not a separate
measurement. Against its own baseline, the catalog index took 25 to 30% off
a cold N=64 open before and 38 to 44% now.

What it does not change:

- On the 9.6-hour archive (one source), the catalog index opens cold no
  faster than the fingerprint: `b-64` 304 ms, `fp` 306 to 309 ms. The
  fingerprint already limits the open to one footer per stream (49
  fetches), and that is cheap at one source. The index pays at many
  sources.
- What remains of a cold N=64 open (about 1.4 s, 245 MB resident) is not the
  index read, which is 0.35 s through the API. I did not break the rest
  down.
- Warm opens with the index, medians before → now: N=1 4.0–5.1 → 3.4–4.7
  ms; N=4 14.7–19.6 → 12.5–17.5; N=16 `a-64` 75.6 → 63.5, `b-64` 109.7 →
  104.9, `a-16` 64.5 → 114.1 (slower, unexplained); N=64 `b-64` 235 → 182,
  `a-64` 1003 → 246, `a-16` 518 → 234 (the last two from wide earlier
  ranges, 270 to 1762 ms).

Runs are single sessions; the earlier run is another session on the same
machine, so cross-run differences carry that drift. No noise floor was
measured beyond the reps shown.

## Reopen

- If a caller's index grows past a page or so per segment, a cold read of
  every index is again proportional to their total size; the table only
  removes the segment BLOBs from the read.
