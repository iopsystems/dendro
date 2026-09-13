---
status: open
opened: 2026-09-11
updated: 2026-09-11
---

# Segment compaction

## Goal

Merge a stream's small segments into larger ones, in place, so that read cost
stops growing with the age of an archive.

Not started. This entry exists so the gap is a known one with a stated
restart condition rather than something each new reader rediscovers.

## Decision Criteria

Build it when someone has an archive whose read time is dominated by segment
count, and can say so with a measurement. Until then the cost is theoretical
and the design below is the expensive part.

**GO** on a measured read that is slower than the same data in fewer segments,
from a workload someone actually runs. **NO-GO**, for now, on reasoning from
first principles — the crate already tunes segment size through
[`seal::SealPolicy`](../../src/seal.rs), and a caller who wants larger segments
can raise the caps before reaching for a merge.

## Scope

In: merging adjacent sealed segments of one stream; renumbering `seq`; the
transaction and crash-safety story for a merge.

Out: cross-stream merges (streams are independent by construction), and
downsampling, which is a different operation — it changes the rows, not just
their packaging, and belongs with `rewrite`.

## Evidence

The crate says three separate times, in its own words, that this matters:

- `src/segment.rs`: "query time tracks segment *count*, which is `crate::seal`'s
  business, not this function's."
- `src/seal.rs`: the seal interval "is also what drives segment count, so the
  trade (loss window against read cost) is the caller's."
- `DESIGN.md`: shortening a batch "multiplies segments, and query time tracks
  segment *count*."

And there is no mechanism: a search of `src/` for a merge or concatenate path
finds none. Segments are created by a seal and destroyed whole by eviction.
`rewrite::copy_sources_into` copies segment BLOBs verbatim and renumbers `seq`
from zero; it never combines two.

So the trade named in those three comments is currently one-way. A caller can
choose larger segments **in advance**, through the seal policy, and cannot do
anything about the segments already written. For a rolling buffer that is fine,
since old segments are evicted. For an archive that is kept, it is not.

## Design and Implementation

Nothing built. Two constraints any design has to answer, both already in the
code:

**`seq` ordering is load-bearing.** `read_segments` is `ORDER BY seq`, and
`db.rs` says so explicitly: "dropping `ORDER BY seq` would silently" produce the
wrong order. A merged segment replacing seq 3..7 either takes one of their
numbers and leaves a gap, or the whole stream is renumbered. A gap is cheaper
and the reader already tolerates it — `rewrite` renumbers precisely because a
filtered copy leaves holes — but that should be stated as a property rather than
assumed.

**A merge must not lower a stream's watermark.** `live_wal` selects
`ts > MAX(last_ts)` over the stream's own segments. A merge that deletes
segments before inserting the merged one lowers that maximum in between, so a
reader landing in the window sees sealed rows resurrected as a live tail — the
same hazard that `db.rs` documents for eviction and `segment_sizes` documents
for a hypothetical "drop these segments" primitive. One transaction, insert
before delete, or both.

The merge itself is a parquet concatenation, which the caller's
[`SegmentEncoder`](../../src/segment.rs) cannot do — it turns WAL rows into a
segment, not segments into a segment. Either dendro learns to concatenate
parquet row groups (it already depends on `parquet` and `arrow`), or the trait
grows a second method and every caller has to implement it. The first is
probably right: concatenation is about the container, not about what a row
means.

## Outcome

Open, not started. Ranked second of four in [what a TSDB has that we do
not](2026-09-12-what-a-tsdb-has-that-we-do-not.md), which honours the GO
criterion above rather than pre-empting it: the measurement comes first.

## Deferred or Reopen Items

- **Reopen** when a measured read is slower than the same data in fewer
  segments, on a real workload.
- Related: this is one of the two gaps that decide whether dendro can sit under
  a time-series database. The other is
  [backfill](2026-09-11-out-of-order-appends.md). Retention, the third thing
  that question raised, was small enough to just do — `evict_streams_before`,
  `segment_sizes` and `archive_bytes` landed with this entry.

## Appendix: Skills Invoked

- `engineering-journal` — this entry, and the index beside it.
