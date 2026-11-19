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
Today every segment is one Parquet file. The candidate alternative is Arrow
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
  drops columns. Both open the bytes with Arrow's Parquet reader. That is
  why FORMAT.md §3.2 requires segment bytes this reader can open.
- **Readers receive what the encoder wrote.** `read_archive` hands segments
  and the materialized tail to callers as encoder bytes, and rezolus's
  reader opens them as Parquet.
- **So the encoder boundary is "rows → Parquet segment",** as the [encoder
  boundary](2026-09-11-encoder-boundary.md) entry concluded. Strictly, a
  segment only has to be Parquet if it will be compacted or projected.

## What the evidence says so far

### The measured cost is the layout, not the format

[Read-optimized finished
archives](2026-09-24-two-forms-live-and-finalized.md) measured on
2026-09-25, on two real recordings converted from `.rez` with segment bytes
unchanged:

| | 1.28 GB, 9.6 h, older agent | 581 MB, 2.3 h, rezolus 5.22.0 |
|---|---|---|
| segments | 5,874 | 1,467 |
| bytes pulled to read every segment's footer | 1,272 MB | 579 MB |
| footer bytes of those | 529 MB (42%) | 423 MB (73%) |

- **Width set the footer's share.** Narrow streams were 1–3% footer. The
  per-task stream was 49% footer at 2,325 columns per segment in the older
  recording, and 88% at 13,941 in the 5.22.0 one.
- **The largest segment.** It held 37,644 columns and 301 rows: 35.6 MB, of
  which 32.1 MB was footer. 27.5 MB of the footer was the `ARROW:schema`
  entry, which carries every task's labels as field metadata.

That cost comes from the wide layout: one column per task, labels in each
column's metadata. rezolus has already decided to change that layout. Its
entry, [The layout of a rezolus dendro
archive](https://github.com/iopsystems/rezolus/blob/main/docs/journal/2026-09-25-dendro-archive-layout.md)
(status: design, nothing built), says:

- **Long tables.** Every table with slots (task, cgroup, per-CPU, device)
  is written long. The columns are `timestamp`, the tick's `:wall_offset`,
  `:window_begin` and `:window_width`, `occupant`, and one column per
  metric.
- **Labels move out of the schema.** Occupant labels go to an occupant
  stream, `<stream>/occupants`. It is a Parquet stream sealed, compacted
  and retained like any other (decided 2026-09-28). Field metadata keeps
  fixed facts only.
- **The gain is measured** in that entry: each column costs about 520 bytes
  of footer per segment of about 300 rows.
  - On the busy host (the 5.22.0 recording), going long and sorted took
    `cpu_usage_task` from 273.3 to 26.3 MB, `syscall_counts_cgroup` from
    12.5 to 1.7 MB, and `cpu_usage_cpu` from 2.8 to 2.1 MB.
  - On the quiet host (the older recording), `cpu_usage_task` went from
    524.0 to 348.7 MB.
- **Row order matters as much as the layout.** One occupant's read from
  the busy host's `cpu_usage_task` took 219.4 MB wide, 37.8 MB long in
  arrival order, and 0.77 MB long sorted by `(occupant, timestamp)`. On the
  quiet host it was 181.3, 496.8 and 4.39 MB. rezolus's converter sorts.
  Whether the writer sorts at seal is not decided.
- **A wide segment can also fail outright.** arrow-rs cannot open a segment
  of 52,109 columns (`TooManyTables`), and long tables avoid that too.

So the wide-segment problem has a design that removes it without changing
the format. The remaining question is whether Arrow IPC beats Parquet for
the long tables rezolus will write.

### What Arrow IPC would and would not change

- **Arrow IPC writes its schema twice.** An IPC file has the Schema message
  at its start and a copy in its footer (`arrow-ipc` 60.0.0, `writer.rs`,
  `footer_builder.add_schema`).
- **Parquet's copy is base64.** Its `ARROW:schema` value is base64 of the
  IPC schema, so each IPC copy is about 3/4 its size (derived).
- **For the old wide layout that would have been worse:** about 41 MB of
  schema in the largest segment, against 27.5 MB (derived). Under the long
  layout the schema is small in both formats.
- **What remains to compare:** column encodings and compression, and decode
  cost on read. dendro's Parquet uses plain encoding with RLE levels and
  `LZ4_RAW` page compression, with dictionary off
  (`segment::writer_props`). IPC uses per-buffer LZ4 or zstd. For long tables sorted by
  `(occupant, timestamp)` I expect Parquet to store fewer bytes. Whether IPC
  reads enough faster to pay for that, I don't know.

## Decision Criteria

Written before the probe runs. The thresholds are proposals for the owner
to confirm or change before it does.

Input: the two recordings above in rezolus's long layout. The probe builds
the layout itself, following the rezolus entry: the same long reshaping
and the same `<stream>/occupants` stream. Each arm encodes both the data
stream and its occupant stream. Rows are sorted by `(occupant, timestamp)`,
as rezolus's converter writes them. Arrival order is also run and
reported, because the writer has not decided to sort at seal. The verdict
uses the sorted run.

Arms, each holding the same rows:

- **P:** Parquet with dendro's current writer properties.
- **P′:** a second, identical run of P. The difference between P and P′ is
  the floor.
- **A:** Arrow IPC file format, LZ4 buffer compression.
- **Az:** Arrow IPC file format, zstd buffer compression.

Measured on each arm, release build, warm page cache, at least 5 runs per
arm, interleaved:

1. Total segment bytes.
2. Bytes read and time to learn every stream's columns at open, both ways:
   2a reading one segment per stream, 2b reading every segment's schema.
   Reported, not used for the verdict.
3. Time to read one occupant's series for one metric across every segment
   of the per-task stream, including the occupant-table lookup.
4. Time to read the whole of the largest per-task segment, by bytes.
5. Encode time per segment, on the writer's seal path.

The floor is the larger of the P-against-P′ difference and 5% of P, per
measure. Results:

- **GO on Arrow IPC (A or Az)** if all of these hold against P:
  - measure 3 or measure 4 is at least 30% faster, beyond the floor;
  - neither measure 3 nor 4 is slower beyond the floor;
  - measure 1 is at most 1.25x;
  - measure 5 is at most 1.5x.
- **NO-GO on Arrow IPC** if neither A nor Az is 30% faster than P on
  measure 3 or 4. Parquet stays, and the encoder boundary stays "rows →
  Parquet segment".
- **No verdict** on any other result. The numbers go to the owner.

**Prediction:** NO-GO. Under the long layout, measure 2 is small for both
formats. Parquet stores the sorted long tables in fewer bytes, and IPC's
decode advantage on measures 3 and 4 stays under 30%.

**Refutation:** A or Az is at least 30% faster than P on measure 3 or 4
within the storage and encode limits, or A or Az stores no more bytes
than P on measure 1.

## Scope

In: the encoding of a sealed segment and of a materialized tail.

Out:

- The container (its own entry).
- The layout of rezolus's tables, which is rezolus's decision and already
  made.
- The WAL row's encoding, which is the caller's.
- Encodings other than Parquet and Arrow IPC (Vortex, Lance, a custom
  columnar format). Each would add a reader dependency rezolus carries.
  Reopen if this probe ends with no verdict.

What a change of encoding would cost, given and not measured:

- Compaction and projection are rewritten for Arrow IPC, or moved behind
  the encoder trait.
- rezolus's reader changes.
- A sealed segment is no longer something any Parquet tool opens. DuckDB
  and pyarrow read Arrow IPC files, but fewer tools do.

## Plan

1. A probe crate beside this entry, like the container probe. It reads
   each recording's segments with dendro's API, reshapes them long, and
   writes each arm.
2. The two recordings measured on 2026-09-25, from local copies matched by
   size. They are not in the repository; this entry records their sizes,
   shapes and the converter version, not their contents.
3. Run on the Mac. The measurements are reads and encodes, not syncs, so
   one platform answers the question. Repeat on Linux only if a result is
   within 5 percentage points of a threshold.
4. Record the numbers and the verdict here.

## Evidence

None yet.

## Outcome

Open. Nothing run.
