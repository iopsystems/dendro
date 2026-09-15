---
status: partly resolved (column-set churn fixed; identity churn open)
opened: 2026-09-13
updated: 2026-09-13
---

# Schema churn becomes segment churn, and where column identity should live

## Goal

Answer a question asked of [compaction](2026-09-11-segment-compaction.md)
once it landed: if a caller's column metadata churns — rezolus re-describing
cgroup columns as cgroups come and go — does that turn into segment churn in
the file?

It does, completely. This entry measures how completely, fixes the half that
is fixable inside the container, and records why the other half is not a
compaction problem at all.

## Decision Criteria

**GO on merging across a schema difference** only where the difference has one
possible meaning. A column present in some segments and absent from others has
one: the rows that lack it took no reading. A column present in two segments
with different *metadata* does not — it may be the same series redescribed, or
a different series wearing a reused name, and the container cannot tell.

**NO-GO on the container guessing.** Fusing two series into one column is not
a policy choice with a downside; it is silent corruption that no later read
can detect. If the meaning is ambiguous, the run stops.

**NO-GO on making union the default.** The assertion it rests on — absence
means no reading — is the caller's to make. A caller who has not thought about
it must get today's behavior.

## Scope

In: measuring the churn, and the merge policy.

Out: rezolus's own column layout, and the secondary index discussed at the
end. Both are above the boundary; this entry records the design so it is not
re-derived, and names what dendro would owe it.

## Evidence

### Measured 2026-09-13: churn of either flavor blocks every merge

One stream, 20 sealed segments of 10 rows each, 20 `i64` columns, compacted
with a target far larger than the whole stream — so a clean archive collapses
to one segment. Three column behaviors, then the same three under the policy
this entry adds:

| the caller's columns | default | unioning |
|---|---|---|
| stable | 20 → 1 (1 merge) | 20 → 1 (1 merge) |
| the column *set* churns | **20 → 20 (0 merges)** | 20 → 1 (1 merge) |
| one column's *metadata* churns | **20 → 20 (0 merges)** | **20 → 20 (0 merges)** |

Not "fewer merges". **Zero.** A run stops at the first schema change, and if
every adjacent pair differs then every run is one segment long, so compaction
is an expensive no-op. The archive keeps every segment it ever sealed, and the
18.2× read penalty the compaction entry measured stays paid in full.

Metadata-only churn is the sharper case: the columns are identical in name,
type, order and count. Arrow compares field metadata in schema equality, so a
single changed key in a single field is as fatal to a run as adding ten
columns. From the outside, nothing about those segments looks different.

### Measured 2026-09-13: unioning pays off at every churn level, by less and less

The table above says union merging restores merging. It does not say whether
that is worth having, because union merging does not make churn free — it
converts it. Segments that would not merge become one segment carrying every
column any of them had, most of them sparse. dendro's own compaction
measurement found the per-segment read cost is footer parsing and scales with
column count, so this trade could in principle give back what it gains.

A second fixture, closer to the caller that raised the question: 40 sealed
segments, 50 columns live at any moment, and `k` of those slots recycled per
segment onto a fresh identity — the cgroup case, where a recycled slot is a
new column rather than a redescribed one. Merged with one target far larger
than the stream, so the whole archive collapses to a single segment. Median
shape over three repetitions, release build:

| slots recycled per segment | segments | merged columns | read before | read after | gain | archive after |
|---|---|---|---|---|---|---|
| 0 | 40 → 1 | 51 | 1.25 ms | 50 µs | 25x | 0.07 MB |
| 1 | 40 → 1 | 90 | 1.31 ms | 72 µs | 18x | 0.08 MB |
| 5 | 40 → 1 | 246 | 1.29 ms | 162 µs | 8.0x | 0.12 MB |
| 25 | 40 → 1 | 1,026 | 1.30 ms | 577 µs | 2.3x | 0.35 MB |

**Unioning is worth doing at every level, and the decay is real.** Half the
slot population turning over every segment still reads 2.3x faster merged than
unmerged, but the 25x of a stable schema is gone, and the archive is five
times the size of the unchurned one. Nulls are cheap, not free.

**And there is no sweet spot to tune to.** The obvious hedge is to merge less
hard, keeping runs short so fewer generations coexist in one segment. At the
heaviest churn arm, merging to 4, 8 and 20 segments instead of 1 gives 2.0x,
1.8x and 1.3x against the full merge's 2.3x, with the archive growing from
0.35 MB to 0.48 MB as the target loosens. Every step away from a full merge is
worse on both axes. The column count does fall as predicted — 1,026 to 276 to
151 to 76 — and it does not buy anything, because the segments it is spread
over cost more than the columns saved.

So the caller's knob is already correct at its existing setting, and the
merge-hardest default needs no qualification.

**The dendro feature this rules out.** The natural next move was for a union
merge to drop columns that are entirely null in the merged output, recovering
the width. It would do nothing here: a recycled slot's column is *sparse*, not
empty — it carries real readings for the segments in which it was live and
nulls elsewhere. A column with no readings anywhere in the merge range does not
arise from churn at all. Not built, and the reason is recorded so it is not
proposed again.

(Both measurements above used scratch fixtures driven through the public API,
not kept in the tree; the committed tests pin the behavior, not the numbers.)

### Three effects compound, and only one of them is compaction

Churn costs more than the blocked merge:

1. **Merging is blocked**, measured above.
2. **Wider segments seal sooner.** The byte and row caps in `seal.rs` are
   fixed, so a schema that grows produces *more* segments as well as
   unmergeable ones.
3. **Every change re-anchors the WAL.** A row carries its group's schema, so
   churn is also bytes in the WAL, on every append that follows a change.

### Why the second flavor is not a compaction problem

The obvious repair — merge anyway, take the newest metadata — is the one thing
that must not happen. In rezolus, column identity lives in remappable numeric
id metadata. Two segments whose `v3` carries a different id are two different
series. Concatenating them produces a column that is one series for the first
half of its rows and another for the second, with no marker at the seam and
nothing in the file that could ever recover the split. Every value is valid,
every row count is right, and `verify` reports the archive sound, because
`verify` does not open a segment.

That is the difference between the two flavors. Column-set churn is a
*packaging* problem, and packaging is the container's job. Identity churn is a
statement that the caller put mutable identity inside an immutable column, and
no merge policy repairs it.

## Design and Implementation

**`rewrite::SchemaPolicy`, on `CompactSpec`.**

```rust
pub enum SchemaPolicy {
    StopAtChange,  // the default: today's behavior
    UnionFields,   // union of the column sets, null-filling
}
```

`CompactSpec::to_rows(n)` is unchanged and still stops at any change;
`CompactSpec::to_rows(n).unioning_fields()` opts in.

Under `UnionFields` a run extends while every field name the segments share is
**identical** — type, nullability and metadata — and grows the merged schema by
the names only some of them carry. Those become nullable, because the segments
that lacked them contribute nulls; a name in every segment of the run keeps the
nullability it had. Rows are widened at write time with
`arrow::array::new_null_array`, so a row that predates a column carries null
rather than a zero standing in for a reading nobody took. That distinction is
the entire reason this is safe: parquet nulls and the query layers above read
them as absent.

Two smaller decisions. A conflicting field is checked across a whole segment
before any of its fields join the union, so a clash late in a segment's field
list cannot leave the union half-extended by the fields before it. And the
existing fast path is preserved: a batch whose schema already equals the merged
one is written unchanged, so `StopAtChange` costs exactly what it did.

`tests/compaction.rs` pins both halves: a fixture that merges 6 → 1 under
union and 6 → 3 under the default, with the null count checked rather than
assumed, and a second fixture whose column metadata moves, which stays 4 → 2
under union.

## Outcome

**Column-set churn is fixed, opt-in, and measured**: 20 → 1 where it was
20 → 20. **Identity churn is unfixed and deliberately so**, with the reason
recorded above rather than left as an unexplained refusal.

The measurement is what makes this entry worth having. "Churn reduces merging"
would have been a reasonable guess and would have been wrong by a category —
it eliminates merging, so compaction silently stops being a feature for
exactly the caller who needs it most.

### The real fix is above the boundary: a secondary index

Proposed while reading the result above, and correct: keep the columns in the
metric tables **dense and static** — a fixed slot per position, carrying no
identity at all — and put the label-to-slot transitions in a secondary index,
keyed by time. A slot means "whatever the index says it meant at this
timestamp".

This is what Prometheus does, arrived at from the other direction: labels live
in an index, chunks carry only values. It dissolves both flavors at once.
Static columns never churn, so every run merges; identity changes become rows
in an index instead of metadata on an immutable column, which is the only
place a mutable fact can correctly live.

**The refinement that matters, and it is not obvious:** those transitions must
**not** live in the per-segment `caller_index` slot this crate already has.
That index is deliberately dropped by a merge and by a column projection —
both documented, both correct, because an index describing one input cannot
describe two. Putting transitions there would mean compaction destroys exactly
the data that makes compaction possible. They have to be time-keyed and
segment-independent: unaffected by a merge, and prunable by retention on
timestamp alone, which the container can do without knowing what a transition
says.

So dendro would owe this design one thing it does not have: a caller-owned,
time-keyed, opaque store — rows of `(source, ts, blob)` the archive never
decodes, evicted by the same predicate that evicts segments. That passes this
repo's scope test, the one [the TSDB
survey](2026-09-12-what-a-tsdb-has-that-we-do-not.md) uses: two callers with
completely different row shapes could both use it, because it stores bytes
against a timestamp and nothing else. The transition *semantics* — what a slot
meant, how a reader resolves one — stay entirely the caller's.

The measurement above is also what sizes this. Unioning leaves a churning
caller at 2.3x where a stable schema gets 25x, and the gap is entirely column
count. A secondary index closes it, because the columns go back to being
static and the merged segment goes back to being narrow. That is the argument
for doing it, and it is a factor of ten rather than a matter of taste.

Not built, and not costless: the caller's read path has to resolve a slot
through the index at a timestamp, which is a real change in rezolus rather
than a storage detail. It is recorded here so the shape is settled before
anyone starts.

## Deferred or Reopen Items

- ~~**Drop all-null columns during a union merge**, to recover the width the
  policy costs.~~ **NO-GO, measured**: churn produces sparse columns, not
  empty ones, so there is nothing for it to drop. Reopen only if a caller
  turns up whose columns really do go entirely silent across a merge range.
- ~~**A time-keyed caller store.** Reopen when a caller commits to the secondary
  index above. The design constraint is fixed: segment-independent, so
  compaction and projection cannot destroy it, and evictable on timestamp
  alone.~~ **Landed 2026-09-15** as the `caller_rows` table, keyed by
  `(source, stream, ts)`, after rezolus committed to the index. Segment-
  independent, evicted by both retention paths, carried by every copy, and
  untouched by compaction; `tests/caller_rows.rs`.
- **Identity churn stays unmerged** until such an index exists. No merge
  policy can fix it, and this entry exists so nobody adds one that pretends to.
- Related: [compaction](2026-09-11-segment-compaction.md) owns the merge and
  its measurement; [the encoder boundary](2026-09-11-encoder-boundary.md) owns
  what the container may know about a row.

## Appendix: Skills Invoked

- `engineering-journal` — this entry, and the index beside it.
