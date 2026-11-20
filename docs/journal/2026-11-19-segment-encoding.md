---
status: resolved
opened: 2026-11-19
updated: 2026-11-20
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
column's metadata. rezolus has since changed that layout, and the change
is built:

- rezolus #1394 (2026-10-02) sends groups of slots long on the agent's
  stream.
- metriken-storage builds long segments and occupant streams in its
  `long_table` and `occupants` modules. They have been in metriken-storage
  since #250 (2026-10-10); `long_table` dates from #178 (2026-09-28) under
  an earlier path.

The design is rezolus's entry, [The layout of a rezolus dendro
archive](https://github.com/iopsystems/rezolus/blob/main/docs/journal/2026-09-25-dendro-archive-layout.md).
Its status still reads "design, nothing built", which is stale; an earlier
draft of this entry repeated it. It says:

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
  quiet host it was 181.3, 496.8 and 4.39 MB.
- **Sorting is an option.** metriken-storage's `LongTableBuilder::finish(sort)`
  orders rows by `(occupant, timestamp)` when asked, and otherwise keeps
  arrival order. Which recording paths ask for it is not checked here.
- **A wide segment can also fail outright.** arrow-rs cannot open a segment
  of 52,109 columns (`TooManyTables`), and long tables avoid that too.

So the wide-segment problem was removed by a layout change, not a format
change. The remaining question is whether Arrow IPC beats Parquet for the
long tables rezolus writes.

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
  cost on read. dendro's own default (`segment::writer_props`) is
  `LZ4_RAW` pages with dictionary off. metriken-storage's archive writer,
  which writes rezolus's segments, seals with zstd level 3, dictionary off,
  plain encoding with RLE levels. IPC uses per-buffer LZ4 or zstd. For long tables sorted by
  `(occupant, timestamp)` I expect Parquet to store fewer bytes. Whether IPC
  reads enough faster to pay for that, I don't know.

## Decision Criteria

Written before the probe runs. The thresholds are proposals for the owner
to confirm or change before it does.

Input, revised 2026-11-19 before anything ran (owner agreed). Two kinds,
because each covers what the other cannot:

- **Real recordings, re-encoded long.**
  - What: the two recordings above, each slotted table's wide segments
    decoded and rebuilt as long segments and `<table>/occupants` streams
    by metriken-storage's own `LongTableBuilder`, the production encoder.
  - What it gives: real value distributions, including the busy host's
    396,117 PIDs.
  - Limit: each long segment holds one wide segment's rows, so it keeps
    the wide writer's segmentation rather than the long writer's own seal
    sizes.
- **Fresh recordings from current rezolus.**
  - What: recorded on a rack VM under the synthetic load rezolus's layout
    entry used on 2026-09-28. That is 300 threads a second, each living
    0.5–5 s, in four named pools; and 5 cgroup jobs a second, each living
    10–60 s; at 1 s and 100 ms sampling.
  - What it gives: the production writer end to end, with its own seals
    and its own row order.
  - Limit: the values are synthetic. The load generator was not
    committed in rezolus, so this probe writes its own from that
    description.

Each arm re-encodes both the data stream and its occupant stream from the
same decoded rows.

**Row order:** production writes long segments in arrival order.
`WriterConfig::sort_long` defaults to `false`, nothing sets it, and
compaction and copies do not sort. So the verdict uses arrival order.
Rows sorted by `(occupant, timestamp)` are run and reported too. As first
written, this paragraph said the verdict used the sorted run. That was
changed after checking the code, before any run.

**If the two inputs disagree** on the verdict, the result is no verdict,
and both go to the owner.

Arms, each holding the same rows:

- **P:** Parquet with dendro's current writer properties. Correction, made
  before the verdict: this was ambiguous, and the first runs read it as
  LZ4_RAW (dendro's `segment::writer_props` and metriken-storage's
  `segment_writer_props`). The archive writer actually seals with
  `segment_props(default_compression())`: zstd level 3, dictionary off.
  P is that. Every result below uses it; the LZ4 runs are superseded (see
  Evidence).
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

1. A probe crate beside this entry, in
   [2026-11-19-segment-encoding/probe/](2026-11-19-segment-encoding/probe/).
   It reads each recording's segments through SQLite, read-only, decodes
   them, and writes each arm.
2. Both inputs as in Decision Criteria. The recordings are not committed to
   the repository; this entry records their sizes, shapes and the rezolus
   version that wrote them, not their contents. The load generator and the
   rack spec that records under it are committed beside the probe.
3. Run on the Mac. The measurements are reads and encodes, not syncs, so
   one platform answers the question. Repeat on Linux only if a result is
   within 5 percentage points of a threshold.
4. Record the numbers and the verdict here.

## Evidence

### Setup

- **Where it ran:** Apple M4 Max, macOS 26.6.2, release build. Warm reads
  from memory: every arm's segments are loaded before timing.
  On Linux x86_64 the gap between Az and P on the syscall table is
  larger, for a reason specific to Parquet's zstd reader; see "Cold-read
  results" below.
- **Arms:**
  - P and P2 are the same bytes, timed separately; their difference is the
    floor.
  - A is Arrow IPC with LZ4_FRAME buffers, and Az is Arrow IPC with zstd
    buffers, both converted from P's decoded rows.
  - Pl, a control not in the criteria, is Parquet with LZ4_RAW. It differs
    from P only in codec.
- **Readers:** every reader decodes one batch per segment, as the IPC
  files hold one batch each.
- **Sessions:** each input ran in 3 separate sessions of 7 interleaved
  runs; the starting arm rotates each run. A session's figure is the median
  of its 7 runs, and the figures below are the median of the 3 sessions,
  with their range.
- **Target:** the occupant measure 3 reads is the one with the most rows,
  the lowest number among ties, so every session reads the same one.

### Inputs

- **Real recordings, re-encoded long.**
  - Sources: the two 2026-09-25 recordings, the quiet older one and the
    busy 5.22.0 one.
  - Tables: `cpu_usage_task`, `syscall_counts_cgroup` and `cpu_usage_cpu`,
    each with its occupant stream.
  - Method: occupant labels are the keys whose values differ among the
    columns of one metric, plus `id`.
  - Occupants found: 6,644 and 396,117 task occupants. These are the
    distinct slot counts in rezolus's layout entry.
  - The occupant stream holds each occupant once, at first sight. The
    production writer also restates the live set every 300 s, so these
    occupant streams are smaller than production's (not measured).
- **The reshape is exact,** by `probe/validate.py`. For every cell of a
  wide segment it checks that the same value is in the long segment, under
  the same occupant labels and timestamp, and that the long segment holds
  nothing else. All OK:
  - quiet `cpu_usage_task`, segments 0, 59 and 158 (62,870, 525,733 and
    208,596 cells);
  - quiet `syscall_counts_cgroup`, segments 0, 57 and 115;
  - busy `cpu_usage_task`, segments 0, 16 and 31 (229,229, 524,707 and
    52,618 cells);
  - busy `syscall_counts_cgroup`, segments 0 and 27.

  Pairing a wide segment with the wrong long segment fails, as it should.
- **Sizes agree with rezolus's 2026-09-27 gate.** In arrival order, at
  LZ4_RAW as the gate wrote:
  - quiet `cpu_usage_task`: 506.6 MB, against the gate's 505.4 MB;
  - busy `cpu_usage_task`: 37.9 MB, against 39.2 MB.
- **Fresh recordings from current rezolus.**
  - Agent and recorder: rezolus `ba47c3f5` (6.0.0-alpha.152), on hv02,
    kernel 6.12.116, experiment 01a1f60f-a9c0-71bd-e9c9-ff8ccefd1061.
  - Load: `probe/rack/loadgen.c`. It spawned 180,000 threads with 10 ms of
    CPU each and 3,000 cgroup jobs.
  - Two recordings, at 1 s and 100 ms intervals.
  - Recording size: the 1 s one is 11.7 MB, with 3 task segments and
    143,203 task occupants. The 100 ms one is 42.3 MB, with 22 task
    segments and 161,222 task occupants.
  - P is the recorded bytes: every column chunk is ZSTD with no dictionary
    page. Parquet does not store the zstd level.
- **The fresh runs differ from rezolus's September spike in two ways.**
  - Fewer task occupants than September's 177,854 and 184,332. 6.0's
    default `task_min_lifetime = 1s` gives no series to threads younger
    than 1 s (inferred, not checked).
  - Per-thread series are off by default in 6.0, so the agent config adds
    `task_attribution = true`.
- **Two earlier rack runs were not usable.**
  - 01a1f5cb: the image's own rezolus 5.31.0 agent held the port, so 5.31.0
    served.
  - 01a1f5ee: the run had no `task_attribution`, so there was no task
    table.

### Results, arrival order (the verdict's)

Bytes are over the three tables and their occupant streams. Times are the
change against P; negative is faster.

| input | P bytes | A / P | Az / P | m3 A | m3 Az | m4 A | m4 Az | m5 A | m5 Az |
|---|---|---|---|---|---|---|---|---|---|
| real, busy | 23,339,255 | 2.12x | 0.97x | −14.9% (−15.0 to −14.3) | −18.9% (−19.2 to −18.7) | −8.5% | −13.6% | −51.6% | −31.6% |
| real, quiet | 222,442,053 | 2.22x | 0.85x | −18.0% (−20.8 to −17.5) | −23.2% (−25.6 to −22.7) | −7.0% | −19.1% | −33.8% | −27.2% |
| fresh, 1 s | 4,630,826 | 2.26x | 1.27x | −30.4% (−31.1 to −29.1) | −21.3% (−22.8 to −20.0) | −16.0% | −15.9% | −51.9% | −21.2% |
| fresh, 100 ms | 11,061,100 | 2.03x | 1.09x | −15.9% (−15.9 to −15.4) | −14.8% (−14.8 to −14.0) | −9.0% | −14.9% | −59.5% | −31.8% |

In arrival order, the P-against-P2 floor was at most 3.4% on measures 3
and 4 in every session, so the floor is 5% everywhere.

Measure 3 bytes, data stream plus occupant stream, P against Az:

| input | P | Az |
|---|---|---|
| busy | 20.5 MB | 20.3 MB |
| quiet | 209.7 MB | 177.6 MB |
| fresh 1 s | 4.16 MB | 5.41 MB |
| fresh 100 ms | 8.09 MB | 9.42 MB |

**Measure 2b** (reported, not in the verdict), reading every segment's
schema, Az against P: −20.5%, −33.5% and −22.1% on busy, quiet and fresh
1 s, and +22.6% on fresh 100 ms.

**Storage, per stream.** Az stores the task data stream in fewer bytes than
P: 0.81x, 0.84x, 0.96x and 0.88x. It stores the task occupant stream in
more: 1.49x, 1.09x, 1.37x and 1.42x. The fresh recordings are mostly
occupant stream: on the 1 s one it is 3.42 MB of P's 4.63 MB. That is why
Az's total is larger there.

Per input, against the criteria:

- **Real, busy: NO-GO.** Neither arm reaches 30% on measure 3 or 4.
- **Real, quiet: NO-GO.** The best is Az at 23.2% on measure 3.
- **Fresh, 1 s: no verdict as written.**
  - A's measure 3 is 30.4% faster (median; 29.1% to 31.1% across
    sessions), but A stores 2.26x and cannot be GO.
  - The NO-GO rule ("neither A nor Az is 30% faster") has no floor, and A
    is 30% faster, so it is not NO-GO either.
  - It becomes NO-GO only if the floor is read into the NO-GO rule too,
    making its bar 35%.
- **Fresh, 100 ms: NO-GO.**

### Prediction

The prediction (NO-GO; Parquet stores fewer bytes; IPC's decode advantage
stays under 30%) held on reads except for A on fresh 1 s. On storage it was
refuted on the real recordings, where Az stores 0.85x and 0.97x of P, and
held on the fresh ones.

### Superseded runs

Two earlier sets of runs are superseded. Their figures are not used above,
and the probe as committed no longer produces them.

- **P at LZ4_RAW.** The first runs read P as LZ4_RAW (see the correction
  under Arms).
  - Against an LZ4 P, Az stored 0.36x to 0.45x on the real recordings.
    That was zstd against LZ4.
  - Under that P, the real recordings were NO-GO: Az's measure 3 was 21% to
    24% faster.
- **Parquet read in 1,024-row batches.** After the P correction, one run
  per input read Parquet on measure 3 in arrow's default 1,024-row batches,
  while IPC read one batch per segment.
  - That run showed Az 30.5% and 34.9% faster on the real recordings, which
    would have been GO.
  - With both formats read one batch per segment, Az is 18.9% and 23.2%
    faster.
  - About a third of the earlier advantage was reader configuration, not
    format.
  - That run also picked the target occupant among ties at random, and the
    busy result varied from 29.2% to 32.1% across reruns.
  - A review caught both problems.

### Sorted order (reported, not the verdict)

Sorted order was measured in every session. The probe does not use
Parquet's page index for a single-occupant read, so sorted Parquet's
single-occupant reads are slower here than a reader that pruned pages
would see. The figures are in the raw output, which the probe regenerates.

## Verdict

**No verdict as the criteria are written.** Decided NO-GO by the owner
on 2026-11-20; see Outcome.

- **Three inputs are NO-GO,** under any reading.
- **Fresh 1 s is no verdict.** A reads 30.4% faster there, so the NO-GO
  rule is not met, and A stores 2.26x, so it cannot be GO.
- **The kinds of input disagree:** the real recordings are NO-GO and the
  fresh ones are not both NO-GO. By the rule written before the runs, the
  overall result is no verdict.
- **It is NO-GO** if the 5% floor is read into the NO-GO rule as well as
  the GO rule. The owner decides.

Plan step 3 asked for a Linux repeat when a result is within 5 points of a
threshold. Fresh 1 s's A (30.4%) and Az's storage there (1.27x against
1.25x) are. The repeat was not run. Inferred, not measured: a Linux result
could move fresh 1 s to NO-GO, but could not make any input GO. Every
other input is at least 6.8 points from the bar, and fresh 1 s fails the
storage limit, which does not depend on the platform.

Correction, 2026-11-20: the cold-read runs on Linux (below) moved one input
toward the bar. On the busy task table, Az reads one series cold 30.8%
faster than P (c3, the cold analogue of measure 3). That is past the 30%
NO-GO bar, so the "6.8 points" above does not hold on Linux. It is under
the 35% that GO needs with the floor, so no input becomes GO.

What the runs show, across all four inputs, warm and from memory:

- **Reads:** Arrow IPC with zstd reads one series 15% to 23% faster than
  Parquet at production's settings, and a whole segment 14% to 19% faster.
- **Encoding:** it encodes 21% to 32% faster.
- **Storage:** it stores 0.85x to 1.27x of P. It is smaller on the real
  recordings and larger on the fresh ones, because of the occupant
  streams.
- **IPC with LZ4** stores about twice P everywhere.

Not measured:

- cold reads from disk (measured later; see "Follow-up: cold reads from
  disk");
- Parquet with dictionary encoding on;
- a reader that prunes Parquet pages;
- rezolus's real query paths, which read through metriken-storage's
  readers;
- the batch size those readers use.

The author's recommendation: keep Parquet. Against production's settings,
IPC's advantage is 15% to 23% on warm single-series reads, at about the
same storage. That does not pay for rewriting compaction and projection,
changing rezolus's reader, and giving up segments any Parquet tool opens.

## Outcome

NO-GO, decided by the owner on 2026-11-20: segments stay Parquet. That
reads the floor into the NO-GO rule, so fresh 1 s is NO-GO too.

The follow-ups below ran after this verdict. Under the owner's reading
they do not change it, but some of their results cut against it:

- **For it:** Parquet's `DELTA_BINARY_PACKED` makes the syscall table
  1.6x to 8.1x larger. Cold reads past the guest's page cache are at most 7%
  of a full scan (whether the Linux host served them from its ZFS ARC or
  the device was not checked) and leave the ranking of P and Az as it was warm.
- **Against it, outside the verdict's measures:** on full scans of the
  syscall table, Az is 26% to 32% faster warm on macOS and 73% to 77%
  faster cold on a Linux x86 VM. On that VM, the cold series read of the
  busy task table is 30.8% faster with Az, past the floor-free NO-GO bar
  and under the 35% that GO needs with the floor.
- **Most of the Linux full-scan gap is, by an estimate from the profile,
  in how parquet 60 builds codecs** (derived; the reopen test in
  "Cold-read results" would confirm it): it creates a zstd decompressor, and a compressor it
  never uses, for every column chunk. That can be fixed upstream (see
  "Cold-read results").
- **One profile of rezolus's viewer** on macOS (not in this tree) put
  segment decode under 7% of the first query's index build. It was not
  measured on Linux, where Parquet decode is 4 to 6 times slower.

Reopen if, after metriken's query-path costs are fixed, segment decode is
more than a quarter of a chart query's time on a real recording, on
macOS or on Linux x86_64.

## Follow-up: Parquet's own encodings, on the syscall count queries

Opened 2026-11-19, after the verdict above, written before running. This is
exploratory, not a second GO/NO-GO.

The owner asked whether another format queries faster and still packs
efficiently, and named the syscall count queries as the workload that
matters. metriken-storage writes every column `PLAIN` with dictionary off,
then zstd 3. Two cheaper changes come first, because neither needs a new
format: Parquet's `DELTA_BINARY_PACKED` encoding on integer columns, and
row order.

- **Arm Pd:** Parquet, zstd 3, dictionary off, `DELTA_BINARY_PACKED` on every
  integer column, re-encoded from the same rows as P.
- **Target table:** `syscall_counts_cgroup` (`syscall/syscall_counts_cgroup`
  on the fresh recordings), with one column per syscall op.
- **New measure 6:** decode every column of every segment of the target
  table, as a query summing across cgroups by op does.
- **Prediction:**
  - In sorted order, Pd stores at least 30% fewer bytes than P.
  - In arrival order, Pd saves less, because one occupant's counter
    values are interleaved with other occupants' and their deltas are
    large.
  - Pd decodes within 10% of P's time on measures 3, 4 and 6.
  - Az stays faster than both on measure 6.
- **Refutation:** Pd saves under 10% in sorted order, or decodes more than
  20% slower than P.

### Follow-up results

Same setup and four inputs as above, 3 sessions of 7 interleaved runs per
input; target table `syscall_counts_cgroup`. Each figure is the median of
the session medians. Bytes are the target table's data stream. Times are
against P; negative is faster. The P-against-P2 floor was at most 4.5% on
measures 3, 4 and 6 in every session.

Arrival order:

| input | Az bytes | Az m3 | Az m4 | Az m6 | Pd bytes | Pd m6 | A m6 | Pl m6 |
|---|---|---|---|---|---|---|---|---|
| real, busy | 1.03x | −7.3% | −26.2% | −26.6% | 2.94x | +43.4% | +60.1% | +56.4% |
| real, quiet | 1.05x | −2.7% | −27.9% | −25.9% | 8.05x | +110.6% | +52.1% | +51.1% |
| fresh, 1 s | 0.97x | −12.9% | −32.2% | −29.6% | 5.50x | +25.7% | +83.0% | +91.7% |
| fresh, 100 ms | 0.92x | −11.7% | −31.9% | −32.0% | 7.23x | +61.2% | +91.2% | +103.6% |

Sorted order: Az is 0.92x to 1.05x of P's bytes and 12% to 33% faster on
measure 6. Pd is 1.56x to 2.51x of P's bytes and 46% to 61% faster on
measure 6.

- **The prediction is refuted.**
  - `DELTA_BINARY_PACKED` makes the table larger than P in every case:
    2.9x to 8.1x in arrival order and 1.6x to 2.5x sorted.
  - Checked independently with pyarrow on one fresh 100 ms sorted
    segment: delta plus zstd 3 is 2.55x the bytes of plain plus zstd 3,
    against the probe's 2.51x on that input.
- **Why delta loses, from the per-column sizes of that segment.**
  - The bulk is the tick-level columns. The long layout repeats
    `timestamp`, `:wall_offset`, `:window_begin` and `:window_width` on
    every occupant's row.
  - Plain encoding leaves those repeats for zstd to remove: `timestamp`
    is 1,561 bytes plain.
  - Delta bit-packs the nanosecond jitter between ticks, which zstd
    cannot remove: `timestamp` is 31,381 bytes with delta.
  - The metric columns are small either way: 94 bytes plain against 123
    with delta for the first op.
- **IPC with zstd (Az) is the only arm faster than P on the full scan
  everywhere.**
  - It is 26% to 32% faster in arrival order, at 0.92x to 1.05x of P's
    bytes.
  - On the single-series read, measure 3, it is only 3% to 13% faster.
  - IPC with LZ4 (A) and Parquet with LZ4 (Pl) are slower than zstd P,
    by 51% to 104% on measure 6. I don't know why LZ4 decodes slower
    here; they hold about twice the bytes.
- **What this suggests, not tested:** the tick-level columns are a
  layout cost. Storing them once per tick, not once per occupant row,
  would shrink and speed up every format. That is rezolus's layout to
  change, not dendro's.

## Follow-up: cold reads from disk

Opened 2026-11-20, written before running. Exploratory, not a GO/NO-GO.

Every measure above decodes bytes already in memory. The owner asked
whether either format allows reads that skip the page cache (`O_DIRECT`
through io_uring), and named the two problems they care about: the memory
it takes to read a `.dendro` file, and query times of several seconds for
one chart. The full scan of the syscall table above takes 6.9 ms (busy)
to 31 ms (quiet) warm with P on macOS, so segment decode explains a small
share of a multi-second chart. Where the rest goes was measured
separately, on rezolus's viewer (see "What this means for the owner's two
problems").

- **Container.** SQLite cannot serve this: dendro reads a segment with
  `SELECT bytes FROM segments` (`src/archive/sqlite.rs`, `read_segment_bytes`),
  through the page cache, and a large blob spans overflow pages. The probe
  models the single-file log container instead: one pack file per arm,
  segments back to back at recorded offsets, each read rounded out to
  4 KiB.
- **Arms.** P (production Parquet, zstd 3), P2 (P again, the floor), Az
  (IPC, zstd), and Au (IPC, uncompressed; decodes without a decompression
  step, so its buffers could be used in place).
- **I/O.** Linux: io_uring, file opened `O_DIRECT`, every read of a measure
  submitted at once. macOS: `pread` one after another on a file with
  `F_NOCACHE`; indicative only.
- **Measures**, on the target table, arrival order:
  - c0: 64 random aligned 4 KiB reads of the pack file, one at a time.
    This is the calibration: it shows whether reads reach the device or
    a cache below the file system.
  - c3: one series across every segment, plus the occupant stream. Only
    the columns needed are read: Parquet column chunks from footers held
    in memory; IPC buffers from footers and message headers held in
    memory.
  - c4: the largest segment, every column, one read.
  - c6: every segment, every column.
  - Each measure reports I/O time, decode time and bytes read.
- **Prediction.**
  - c6: I/O is under half of decode time for every arm, so the cold scan
    is within 1.5x of the warm one, and Az stays faster than P.
  - c3: P reads fewer bytes than Az, but both cold single-series reads
    stay under 2x their warm time.
  - Au reads about twice Az's bytes and decodes fastest.
- **Refutation.** I/O exceeds decode time on c6 for P or Az, or the cold
  ranking of P and Az on c6 differs from the warm one.

### Cold-read results

- **Where it ran.**
  - Linux: an anvil VM on hv02 (`auto.c`, debian-13-6.12, kernel
    6.12.116, 24 vCPUs, clocksource tsc). Packs on a 20 GiB ext4 scratch
    disk, a zvol attached `cache='none'`. Experiments
    01a1fa1b-2f9b-718d-7b51-d86ac76a09d8 (cold reads),
    01a1fa1d-0e55-7105-ca4b-3fd2c4ad9a1b (`target-cpu=native`) and
    01a1fa22-d319-716e-cc34-7028847340bf (perf).
  - macOS: the M4 Max above, internal NVMe.
- **Inputs:** the four inputs above, the syscall table on all four and
  the task table on three. The quiet task table was left out: its packs
  are 376 MB of input.
- **Sessions:** 3 sessions of 7 interleaved runs, as above; figures are
  the median of the three session medians. The P-against-P2 floor on c3
  and c6 was under 4% on those medians everywhere, and up to 10.1% in
  one session. On c4, one 38 KB segment, P and P2 differed by 32% to 34%
  in every session on the busy syscall table, a systematic difference
  rather than noise, so c4 is not used below.
- **Where the reads were served from.**
  - Linux: c0, one 4 KiB read at a time, was 75 to 81 us in every arm,
    flat across packs from 0.25 MB to 55 MB. So the reads left the
    guest. Whether the host served them from its ZFS ARC or from the
    NVMe device was not checked: `cache='none'` bypasses the host's page
    cache but not the ARC, and each pack is read again in each of 7
    runs after being written. `zfs get primarycache` on the zvol, or ARC hit counts
    during a run, would settle it. Until then, the I/O figures below
    may understate a read from the device.
  - macOS: writing the pack with `F_NOCACHE` was needed. Before that, c0
    was 0.9 us, so reads came from the cache. After it, c0 was 19 to 27
    us on packs up to 1.4 MB and 72 to 99 us on packs of 4 MB or more.
    A latency that depends on the file's size suggests a cache for the
    small packs; I don't know which.
- **Au is out.** Uncompressed IPC is 13 to 173 times P's bytes (data
  plus occupant stream), 55 MB for the busy syscall table against 0.96
  MB. Inferred from the format, not measured: Arrow keeps a value slot
  for every null cell. Its packs went over the probe's 64 MB limit on
  four of the seven tables. Where it ran, I/O was 53% to 89% of its time.

Linux, c6 (every segment, every column), median ms:

| table | P io | P decode | Az io | Az decode | io share | Az vs P |
|---|---|---|---|---|---|---|
| busy syscall | 0.56 | 40.1 | 0.49 | 8.9 | 1.4% / 5.2% | −76.9% |
| quiet syscall | 1.24 | 171.9 | 1.19 | 41.9 | 0.7% / 2.8% | −75.1% |
| fresh 1 s syscall | 0.27 | 11.2 | 0.21 | 2.9 | 2.4% / 6.7% | −73.1% |
| fresh 100 ms syscall | 0.71 | 110.4 | 0.60 | 25.1 | 0.6% / 2.3% | −76.9% |
| busy task | 3.19 | 284.5 | 2.71 | 192.0 | 1.1% / 1.4% | −32.3% |
| fresh 1 s task | 0.62 | 12.8 | 0.45 | 9.5 | 4.6% / 4.6% | −26.1% |
| fresh 100 ms task | 1.49 | 112.0 | 1.60 | 74.9 | 1.3% / 2.1% | −32.5% |

macOS, same measure: I/O is 0.7% to 4.2% of c6 for P and Az; Az is 25%
to 32% faster than P on the syscall tables and 13% to 14% on the task
tables. On c3, macOS issues its reads one at a time, and I/O is 45% of
P's time on the quiet syscall table (4.0 ms of 9.1). On Linux, with
every read submitted at once, c3 I/O is at most 7% of P's time and at
most 17% of Az's.

- **The prediction holds for I/O.** c6 I/O is under half of decode for
  P and Az on every table and both systems; it is at most 7% of the
  total. Cold reads leave the ranking of P and Az as it was warm.
- **The prediction on c3 bytes mostly holds:** P reads fewer bytes than
  Az on five of seven tables, because Parquet reads only the three column
  chunks. The quiet syscall table is equal. On the busy task table P
  reads 3% more (20.7 MB against 20.1), because its three column chunks
  (15.8 MB) outweigh Az's buffers (12.8 MB); Az's occupant stream is the
  larger (7.3 MB against 4.9).
- **The prediction on Au is refuted.** It reads 13 to 173 times P's
  bytes, not about twice Az's, and on Linux it decodes the fresh 1 s
  syscall table slower than Az (3.4 ms against 2.9).
- **The prediction on c3 time holds on macOS for the syscall table:**
  cold single-series reads take 0.85x to 1.07x their warm measure-3 time
  (P busy 1.92 to 1.80 ms, quiet 8.45 to 9.05; Az busy 1.77 to 1.51,
  quiet 8.29 to 8.32). This is approximate, because the cold path holds
  footers in memory. Not compared on Linux, where measure 3 did not run
  warm.
- **Parquet decodes the syscall table 4 to 6 times slower on Linux than
  on macOS** (busy: 40.1 ms against 6.9). Az is 1.5 to 1.8 times slower.
  On the task table, with 6 columns against 21 or 22, P is 1.6 times
  slower and Az 1.2 to 1.4 times.
- **Refuted: missing x86 SIMD.** A build with `-C target-cpu=native`
  (the host has AVX2 and BMI2) gives the same times as the default
  build: busy syscall c6 decode 40.6 ms against 40.0.
- **The cause, from a perf profile** of the cold run on the busy syscall
  table (`perf record -e cpu-clock -g`, all four arms):
  - 19.0% of samples are in `ZSTD_createDCtx` and 18.3% in
    `ZSTD_createCCtx`, against 12.8% in zstd's sequence decoding.
  - parquet 60's `ZSTDCodec::new` (`src/compression.rs`) builds a
    `zstd::bulk::Compressor` and a `Decompressor`, and the reader builds a
    codec for every column chunk (`create_codec` in
    `src/file/serialized_reader.rs`). A scan of the busy syscall table
    creates 588 of each (28 segments, 21 columns), and the compressor is
    never used.
  - arrow-ipc 60 keeps one zstd decompression context per decoder,
    which is why Az does not pay this.
  - These shares are of one profile: the busy syscall table, all four
    arms, the whole process including the probe's re-encoding. On the
    task tables, with 6 columns, context creation explains much less of
    the Linux slowdown (derived: about 10 ms of the busy task table's
    63 ms excess, where the excess is P's Linux decode less P's macOS
    decode scaled by Az's Linux-to-macOS ratio, 284.5 − 178.5 × 1.24).
  - Why this costs more on the Linux VM than on macOS is not known. The
    page faults with attributed callers are under the probe's own read
    buffers,
    not the zstd contexts. One candidate: both context constructors
    execute CPUID (zstd 1.5.7, `ZSTD_cpuSupportsBmi2`), which traps to
    the hypervisor in a VM. A bare-metal x86 run would tell the two
    apart.
  - Reopen condition for this explanation: a parquet build that reuses
    one zstd decompression context per file and builds no compressor
    on read, run on the same host, should bring P's c6 decode of the
    busy syscall table within 1.5x of Az's. If it does not, the
    explanation is wrong.

### What this means for the owner's two problems

- **Neither format nor cold I/O explains multi-second charts.** A cold
  full scan of the largest syscall table (quiet) costs 1.2 ms of I/O on
  Linux, with the caveat on where the reads were served from.
- **The Parquet codec cost is real and can be removed upstream:** reusing
  one decompression context per reader, as arrow-ipc does, and not
  building the compressor on read should remove most of the Linux VM's
  gap between P and Az on the syscall table without a format change.
  Making only the compressor lazy would remove about half of the context
  cost.
- **A separate profile of rezolus's viewer** (rezolus c9e043ad,
  metriken-storage 0.13.0, metriken-query 0.50.0; M4 Max; not in this
  tree) found where the seconds and the memory go. This is context for
  the owner's question, not part of this entry's evidence:
  - The first query on a table builds an in-memory index of every
    series: 2.0 s for the task table of the fresh 100 ms recording, 3.8 s
    on the 9.6 h one. 85% to 99% of that is copying and hashing
    per-series label maps, and segment decode is under 7%.
  - After one task-table query RSS is 1.94 GB, of which 860 MB is copied
    `BTreeMap<String, String>` label maps for 161,222 series.
  - Warm `sum` of `irate` queries take 0.37 to 0.68 s, mostly stepping
    every series through every grid point in metriken-query's
    `batch_rate.rs`.
  - Those belong in metriken's journal, with a baseline, before any
    change.
