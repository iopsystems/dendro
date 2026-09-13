---
status: in-progress
opened: 2026-09-12
updated: 2026-09-12
---

# What a TSDB has that we do not, and which of it is ours to build

## Goal

Survey what sophisticated time-series databases provide, decide which of it
belongs in a storage container rather than in the layers above one, and work
the part that does.

The point is not a feature list. It is to make each absence a decision with a
reason attached, so that the next person comparing dendro to Prometheus or
Influx can tell "we chose not to" from "nobody has got to it", and so the
README stops describing one as the other.

## Decision Criteria

**In scope** when the feature is about *storing, cataloguing, or handing back*
bytes, and can be built without the container knowing what a row means.

**Out of scope** when it needs value semantics (is this a counter? a gauge? a
histogram?), belongs to the query engine, or contradicts a stated non-goal
(one writer, one file, no cluster).

**The test that settles most cases**: could two callers with completely
different row shapes both use it? If only a metrics caller could, it belongs
above the encoder boundary.

## Scope

In: the survey and its classification, and the items it finds are ours.

Out: anything the classification puts above the boundary. Named below with
the reason, so that the list is a decision rather than an oversight.

## Evidence

Surveyed against Prometheus block storage, Thanos/Mimir, InfluxDB's TSM,
VictoriaMetrics, and the lakehouse formats — the last because dendro is
structurally closest to them: a catalog of immutable columnar files, which is
what Iceberg and Delta are.

**Already here, and two of them better than typical.** Per-append durability
with a WAL that is readable rather than a staging area; snapshot isolation for
readers; an exact copy of a live archive; immutable segments copied verbatim
by combine and filter; crash recovery, now under test. Retention takes a
per-stream predicate, where most systems offer one global window. Dropping a
stream entirely is already expressible, through retention or a filtered copy.
Series churn needs no special handling because streams are transient by
construction and vanish when their last row is evicted.

**Missing, and ours.** Four, ranked by value:

1. **Nowhere for the caller's index to live.** The README calls this a
   boundary, and half of it is: dendro cannot know what a series is. The
   other half is a gap. A database built on dendro "brings its own index" and
   has nowhere to put it, so it becomes a sidecar file and the single-file
   property — the thing the whole container is shaped around — dies with it.
   Storing an opaque blob the archive never reads is exactly a container's
   job, no different from the segment bytes beside it.

   The demand is not hypothetical. rezolus's read-path work records that
   caching per-stream metric names in the catalog would remove its last fixed
   open cost, and `read::probe` still opens one parquet footer per stream at
   open to learn what a stream holds. With somewhere to put an index, that is
   one catalog read.

2. **No compaction.** Read cost tracks segment count and nothing merges, so a
   kept archive degrades forever; a rolling buffer escapes only because
   eviction removes old segments. Container-level work — concatenating
   parquet row groups, which dendro already depends on both crates for. It
   has its own entry, [segment compaction](2026-09-11-segment-compaction.md),
   whose GO criterion is a measured read slower than the same data in fewer
   segments. That gate is honoured here rather than pre-empted: this survey
   does not build it.

3. **No integrity check.** An archive is an artifact you hand someone, and
   there is no way to ask whether one is sound short of reading all of it.
   SQLite's own check is not exposed, and nothing validates that a segment
   blob is even parquet. Cheap to add and disproportionately useful for a
   format whose selling point is that the file travels.

4. **Segments have arbitrary boundaries.** They close on bytes, rows, or age
   since opening. Block-oriented stores align to wall-clock buckets because
   it makes range pruning predictable and makes two archives of the same
   window comparable segment for segment. A fourth seal option, small, and it
   composes with compaction when that lands.

A fifth, smaller: introspection is spread across five entry points
(`page_stats`, `archive_bytes`, `segment_sizes`, `total_rows`,
`read::catalog`), so every consumer rebuilds "describe this archive".

**Missing, and not ours.** Grouped by why:

- *Needs value semantics.* Downsampling and rollups; an inverted index over
  labels; deleting one series; exemplars; native histograms. Each requires
  knowing what a value means, which is the one thing the container must not
  learn. See [the encoder boundary](2026-09-11-encoder-boundary.md).
- *Belongs to the engine above.* PromQL, query caching, planning, predicate
  pushdown, rate and alignment semantics.
- *Contradicts a non-goal.* Replication, sharding, clustering. One writer,
  one file, stated from the start.
- *Chosen differently, on purpose.* Gorilla-style float encodings beat
  parquet's generic ones on time series; parquet is the segment format for
  its ecosystem and for the blob model, and `CopySpec::writer_props` now lets
  a caller tune within it.

Two are adjacent rather than closed. Reading from object storage needs an
async VFS — the same wall the browser work hit — while *shipping* an archive
there already works, since it is one portable file. And deduplicating
redundant producers, which Thanos does for replica pairs, is already designed
here as the producer-epoch merge: dendro carries the keys, nothing consumes
them. See [generations](2026-09-12-generations-reset-versus-wrap.md).

## Design and Implementation

Recorded per item as it lands.

**1. Somewhere for the caller's index (landed).** A segment carries an
opaque `index: Option<Vec<u8>>` — returned by the encoder alongside the
bytes, stored in a nullable `segments.caller_index` column, and never read by
the archive. Read back two ways: `Db::read_segment_indexes` answers from the
catalog for sealed segments without touching a payload, and
`read::stream_indexes` also materializes the live tail, whose index does not
exist until its segment does, and returns them in the order
`read::stream_segments` returns the segments they describe.

Three decisions worth keeping. The index rides on `Segment` rather than
arriving through a separate trait method, so it is computed from the same
rows as the bytes and a reader materializing a tail cannot drift from the
writer that will seal it. `insert_segment` keeps its existing shape and
`insert_segment_with_index` is the second spelling, because an index is
opt-in and `None` at every call site would be noise. And a **column
projection drops the index** rather than copying it: the copy has fewer
columns than the index was built over, and only the caller can rebuild it.

Additive and nullable, so `SCHEMA_VERSION` stays 4 and a legacy archive
reads `NULL` through its compatibility view. `tests/caller_index.rs`.

## Outcome

In progress.

## Deferred or Reopen Items

- **Compaction** stays gated on its own entry's measurement.
- The out-of-scope list above is the deferral for everything else; each line
  carries the reason it would have to stop being true.

## Appendix: Skills Invoked

- `engineering-journal` — this entry.
