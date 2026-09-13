---
status: implemented
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

### Measured 2026-09-13: read cost tracks segment count, and so does size

The GO criterion above asked for "a measured read that is slower than the
same data in fewer segments". It is, by a lot.

`src/bin/measure-compaction.rs` writes one body of data several times —
identical rows, columns and encoder — differing only in how often the caller
seals, then reads each archive the way a consumer does: every segment's bytes
fetched, every segment's parquet footer parsed, which is the work a planner
must do before it can answer anything. Arms are **interleaved** rather than
run in sequence, so machine load lands on all of them, and the median of
seven repetitions is reported. 20,000 rows of 50 `i64` columns:

| rows/segment | segments | read + parse | catalog only | archive |
|---|---|---|---|---|
| 50 | 400 | 12.26 ms | 452 µs | 9.92 MB |
| 250 | 80 | 2.96 ms | 239 µs | 5.29 MB |
| 1,000 | 20 | 1.22 ms | 184 µs | 4.43 MB |
| 5,000 | 4 | 721 µs | 177 µs | 4.21 MB |
| 20,000 | 1 | 674 µs | 182 µs | 4.17 MB |

**18.2× slower to read and 2.38× larger**, finest against coarsest. Three
runs gave 18.07×, 18.18× and 17.58× — the spread is noise, the gap is not,
and the machine was not idle (load average 5.8), which interleaving is there
to absorb.

**The relationship is linear in segment count, which is the claim.** Fitting
fixed + per-segment to the extremes gives **674 µs + 29.0 µs per segment**,
and it predicts the middle of the table to within 3% (80 segments: 2,997 µs
predicted against 2,960 measured; 20 segments: 1,255 against 1,220).

**And the per-segment cost is footer work, not something else.** Re-running
at 10 columns instead of 50 moves the slope from 29.0 µs to **7.8 µs** per
segment — it scales with column count, which is what a parquet footer is
made of. The same run still shows 13.0× end to end.

**Size has the same shape and a separate cause**: 2.38× at both column
counts. Each segment carries a complete footer, and compression cannot work
across a segment boundary, so a stream cut 400 ways pays both 400 times.

One thing the table also shows that the entry did not predict: **dendro's own
catalog read grows too**, 182 µs to 452 µs, because the catalog has 400 rows
where it had one. Smaller than the footer cost by an order of magnitude, and
in the same direction.

**Verdict: GO.** The trade the three comments name is real, it is large, and
it is one-way without a compactor.

### The claim, before it was measured

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

**Built 2026-09-13**, `rewrite::compact` and `compact_stream`, taking a
`CompactSpec` (a target row count, and optional writer properties for the
merged segment). Both constraints below were answered the way this entry
predicted, and a third turned up in measurement that it did not.

**`seq` ordering.** A merged segment takes the run's FIRST seq and the rest
of the run's numbers are left unused, so numbering stays ascending with
gaps. The entry guessed a gap would be cheaper than renumbering and that
readers already tolerate one; both hold — `read_segments` is `ORDER BY seq`
and `rewrite` already produces gaps.

**The watermark.** The entry worried that deleting before inserting lowers
`MAX(last_ts)` in between, so a reader landing in the window would see
sealed rows resurrected as a live tail. One transaction settles it: other
connections see the whole merge or none of it, and the merged segment's
`last_ts` equals the run's last anyway. `Tx::delete_segment` exists for this
and is documented as the reason it is on `Tx` at all, unlike the WAL prune.

**A run stops at a schema change**, rather than reconciling two shapes: a
stream's schema may drift, and dendro concatenates parquet rather than
understanding it. Segments either side of a change stay separate. That is
still the default, and it proved far more expensive than this entry expected
for a caller whose columns churn — see [schema churn and column
identity](2026-09-13-schema-churn-and-column-identity.md), which measures it
and adds an opt-in union policy for the half that is safe to merge.

**The merged segment carries no index**, for the same reason a column
projection carries none: the index described one input, and combining two
needs to know what they mean.

**Parquet, not a trait method.** As this entry proposed: concatenation is a
property of the container, not of what a row means, so `concat_parquet`
decodes and re-encodes rather than every caller implementing a merge. The
cost is that compaction, like column projection, requires segments to really
be parquet.

### The finding measurement turned up: merging alone gives back no space

Compaction was justified partly on size, and the first working version
delivered none of it:

```
compacting the 400-segment arm: 400 -> 1 segments in 60.67ms;
read 12.10ms -> 643.42µs (18.80x); archive 9.92 MB -> 9.92 MB (1.00x)
```

SQLite does not return deleted pages to the filesystem; they go on the free
list to be reused. The read cost was recovered in full and the file was
exactly as large as before — the opposite of what anyone compacting would
expect, and invisible without measuring it, because every test that checks
rows and catalogs passes either way.

So the archive-wide `compact` reclaims at the end, uncapped, which it can
afford because compaction already requires that nothing else is writing.
`compact_stream` does not, and says so. After the fix:

```
compacting the 400-segment arm: 400 -> 1 segments in 69.60ms;
read 12.10ms -> 650.46µs (18.61x); archive 9.92 MB -> 4.19 MB (2.37x)
```

**Compacting an archive now lands on the same artifact as writing it coarse
in the first place** — 650 µs against 708 µs, 4.19 MB against 4.17 MB — which
is the strongest statement this could end on.

### The constraints, as they stood before it was built

Two constraints any design has to answer, both already in the
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

**Implemented 2026-09-13**, after the measurement it was gated on passed
decisively. `tests/compaction.rs` covers rows and order preserved, the
catalog agreeing, the watermark and live tail untouched, a run stopping at a
schema change, the index dropped, idempotence, one stream at a time, and the
reclaim. The measurement lives in `src/bin/measure-compaction.rs` and is
rerunnable.

The order mattered. Ranked second of four in [what a TSDB has that we do
not](2026-09-12-what-a-tsdb-has-that-we-do-not.md), which honoured this
entry's gate rather than pre-empting it — and the measurement then paid for
itself twice: once by justifying the work, and once by catching that the
first working version returned no space at all.

## Deferred or Reopen Items

- ~~**Reopen** when a measured read is slower than the same data in fewer
  segments.~~ **Done**: 18.2× slower and 2.38× larger, reproducible, with
  the per-segment cost identified as footer parsing.
- **A run stopping at a schema change turned out to be the dominant cost for
  a churning caller**, not a corner: measured at zero merges out of twenty.
  Half of it is now fixable through `CompactSpec::unioning_fields`; the other
  half is not a compaction problem. See [schema churn and column
  identity](2026-09-13-schema-churn-and-column-identity.md).
- Related: this is one of the two gaps that decide whether dendro can sit under
  a time-series database. The other is
  [backfill](2026-09-11-out-of-order-appends.md). Retention, the third thing
  that question raised, was small enough to just do — `evict_streams_before`,
  `segment_sizes` and `archive_bytes` landed with this entry.

## Appendix: Skills Invoked

- `engineering-journal` — this entry, and the index beside it.
