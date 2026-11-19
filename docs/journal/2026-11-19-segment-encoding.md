---
status: open
opened: 2026-11-19
updated: 2026-11-19
---

# Segment encoding: whether Parquet is the right format inside a segment

## Goal

Decide what encoding a sealed segment should use in the single-file
container
([2026-11-18-single-file-log-container.md](2026-11-18-single-file-log-container.md)).
That container treats a segment as an opaque payload, so the encoding can
be chosen on its own merits.

Today every segment is one Parquet file. This entry asks whether that suits
the segments dendro's one caller (metriken-storage, recording rezolus
data) actually writes: short and often very wide. The candidate is Arrow
IPC.

## How much dendro depends on Parquet today

From the code at 3a0246b:

- **Storing and reading need nothing from Parquet.** The caller's
  `SegmentEncoder` produces the bytes and reports the segment's row count
  and first and last timestamps. The catalog stores the reported values; it
  never parses the bytes. Container tests store segments whose bytes are
  `b"not-parquet"` (`src/archive/sqlite.rs`, search for that literal).
- **Two optional operations decode Parquet.** Compaction
  (`rewrite::compact_stream`) decodes a run of segments and re-encodes it,
  and copy with a column projection (`rewrite::project_segment_columns`)
  drops columns. Both open the bytes with Arrow's Parquet reader. FORMAT.md
  §3 therefore requires segment bytes that this reader can open.
- **Readers receive Parquet.** `read_archive` hands segments and the
  materialized tail to callers as Parquet, and rezolus's reader opens them
  as Parquet.
- So the encoder boundary is "rows → Parquet segment", as the
  [encoder boundary](2026-09-11-encoder-boundary.md) entry concluded.
  Strictly, a segment only has to be Parquet if it will be compacted or
  projected.

## What the evidence says so far

All measured in [read-optimized finished
archives](2026-09-24-two-forms-live-and-finalized.md) on 2026-09-25, on two
real recordings converted from `.rez` with segment bytes unchanged:

| | 1.28 GB, 9.6 h, older agent | 581 MB, 2.3 h, rezolus 5.22.0 |
|---|---|---|
| segments | 5,874 | 1,467 |
| bytes pulled to read every segment's footer | 1,272 MB | 579 MB |
| footer bytes of those | 529 MB (42%) | 423 MB (73%) |

- **Width sets the footer's share.** Narrow streams were 1–3% footer. The
  per-task stream was 49% footer at 2,325 columns per segment in the older
  recording, and 88% at 13,941 in the 5.22.0 one.
- **Most of the footer is the embedded Arrow schema.** The widest segment
  held 37,644 columns and 301 rows. It was 35.6 MB, of which 32.1 MB was
  footer, and 27.5 MB of that was the `ARROW:schema` entry: the serialized
  Arrow schema, carrying every task's labels as field metadata, repeated in
  every segment.
- **Switching format alone may not fix this.** An Arrow IPC file also
  starts with its schema, field metadata included. Moving to Arrow IPC with
  the same schema would carry the same 27.5 MB. The rest, 32.1 − 27.5 =
  4.6 MB for 37,644 columns, is about 120 bytes per column of Parquet's own
  metadata; that is derived, not measured.
- **So there are two questions, not one.** Where the labels live (in every
  segment's schema, or once, in the catalog or `caller_rows`) is an
  encoder decision. Which format holds the columns is a format decision.
  The probe must separate them, or it will credit Arrow IPC with a saving
  that comes from moving metadata.

## Decision Criteria

Written before the probe runs. The thresholds are proposals for the owner
to confirm or change before it does.

Arms, each re-encoding the same recordings' segments with the same rows:

- **P:** Parquet as today: the current encoder, metadata in each schema.
- **P−m:** Parquet with per-field label metadata removed from the schema
  and stored once per stream elsewhere.
- **A:** Arrow IPC file format with the same schema as P, LZ4 buffer
  compression.
- **A−m:** Arrow IPC with P−m's schema, LZ4 buffer compression.

Measured on each arm, release build, warm page cache, at least 5 runs per
arm, interleaved:

1. Total segment bytes.
2. Bytes read and time to learn every stream's columns at open.
3. Time to read one series (one column) across every segment of the wide
   per-task stream.
4. Time to read the whole of one wide segment.
5. Encode time per segment, on the writer's seal path.

**GO on moving label metadata out of the schema** (an encoder change, with
a place in the format to put it) if P−m reads at least 50% fewer bytes
than P on measure 2, on the 5.22.0 recording.

**GO on Arrow IPC** if A−m against P−m:

- cuts measure 2 or measure 3 by at least 30%;
- is no slower on measure 4;
- grows measure 1 by at most 25%.

**NO-GO on Arrow IPC** if A−m is within 30% of P−m on measures 2 and 3.
Parquet then stays, and the gain, if any, comes from the metadata move.

**Prediction:** most of the open-time saving comes from P−m. Against P−m,
A−m reads columns faster (no Parquet page decoding) and stores more bytes
(LZ4 buffers against Parquet's encodings). Whether its read gain clears
30% I don't know.

A result where A shows most of A−m's saving refutes the prediction that
the metadata dominates.

## Scope

In: the encoding of a sealed segment and of a materialized tail, and where
per-column metadata lives.

Out:

- The container (its own entry).
- The WAL row's encoding, which is the caller's.
- Encodings other than Parquet and Arrow IPC (Vortex, Lance, a custom
  columnar format). Each one adds a reader dependency rezolus would carry.
  Reopen if neither arm meets the criteria.

What a change of encoding costs, given and not measured:

- Compaction and projection are rewritten for Arrow IPC, or moved behind
  the encoder trait.
- rezolus's reader changes.
- A sealed segment is no longer something any Parquet tool opens. DuckDB
  and pyarrow read Arrow IPC files, but fewer tools do.

## Plan

1. A probe crate beside this entry, like the container probe. It reads
   each recording's segments with dendro's API and writes the four arms.
2. The two recordings measured on 2026-09-25, from local copies matched by
   size (1,280,499,712 and 581,300,224 bytes as `.rez`). They are not in
   the repository; the entry records their sizes, shapes and the converter
   version, not their contents.
3. Run on the Mac; the measurements are of reads and encodes, not fsync, so
   one platform answers the question. Repeat on Linux only if the result is
   close to a threshold.
4. Record the numbers and the verdict here.

## Evidence

None yet.

## Outcome

Open. Nothing run.
