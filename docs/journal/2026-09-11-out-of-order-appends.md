---
status: open
opened: 2026-09-11
updated: 2026-09-11
---

# Out-of-order appends are accepted and never read

## Goal

Decide what dendro does with a row whose timestamp is older than its stream's
newest sealed segment. Today it stores the row, charges you the space, and never
shows it to anyone — with no error.

Two separable questions, and they should not be answered together:

1. **Should it be loud?** A silent drop is a trap whatever the answer to (2).
2. **Should it be supported?** That is backfill, and it changes the rule the
   read path rests on.

## Decision Criteria

**(1) loudness** — do it as soon as anyone appends to dendro from a source that
is not strictly monotonic. That is any producer with retries, buffering, a
clock that can step, or more than one thread staging rows. The bar is low
because the failure is silent.

**(2) backfill** — GO only if a caller needs late samples *and* can accept
whatever the seam rule below becomes. NO-GO on "a time-series database usually
supports it": the watermark is load-bearing, and trading it away for a use case
nobody has is how a container stops being reliable at the one thing it does.

## Scope

In: what happens at `insert_wal_rows` when `ts` is at or below the stream's
sealed watermark.

Out: out-of-order rows *within* one un-sealed batch — those seal into one
segment together and the encoder can order them however it likes. Also out:
duplicate timestamps, which the `wal` primary key `(source_id, stream, ts)`
already rejects loudly.

## Evidence

Measured against the real writer, 2026-09-11. Append at ts 100, 200, 300; seal;
then append 150 and 400:

```
rows physically in the wal table: 2      (150 and 400)
rows the watermark calls live:   1      (400)
what a reader sees: ["100,200,300", "400"]
```

ts=150 is durably committed, occupies space for the life of the archive, and
cannot be read through any path in the crate. No error is returned, no warning
is logged, and `read_wal` will show it to anyone who goes looking — so the row
is visible to a debugger and invisible to a reader, which is the worst of the
available combinations.

The mechanism is [`LIVE_WAL_PREDICATE`](../../src/db.rs): a WAL row is live iff
`ts > COALESCE(MAX(last_ts) of that stream's segments, 0)`. That predicate is
not incidental. It is the entire reason the seal seam needs no coordination: the
prune that follows a seal runs *outside* the seal transaction, so the `wal`
table routinely still holds rows a sealed segment already covers, and the
watermark is what stops a reader splicing them in twice. `db.rs` calls the prune
"a pure background optimisation with no correctness role" precisely because of
it.

## Design and Implementation

Nothing built.

**For (1), loudness.** The cheapest honest options, in order of how much they
cost a caller:

- Return the count of shadowed rows from `wal`/`insert_wal_rows`, so a caller
  can assert it is zero. Cheap, non-breaking, easy to ignore — which is also the
  objection.
- Reject the append. Correct-by-default, and it makes the watermark a documented
  precondition rather than an implementation detail. Costs a read of the
  watermark per append unless the writer caches the per-stream maximum it has
  sealed, which it already tracks for `seq`.
- Log and continue. Cheapest, and the worst: it puts the finding somewhere
  nobody is reading at 3am.

Second option looks right, with the count as the escape hatch for a caller who
genuinely wants best-effort.

**For (2), backfill.** The watermark cannot simply be relaxed. Candidates, none
costed:

- **A per-stream open region.** Do not seal up to the newest row; leave a
  configurable lateness window unsealed. Bounded lateness only, and it enlarges
  the un-sealed tail, which is what an unclean kill loses.
- **Out-of-order segments with an explicit time index.** Let a late row seal into
  its own segment, and make the reader order by `first_ts` rather than `seq`.
  That breaks a documented invariant — `ORDER BY seq` is load-bearing today — and
  it means segments can overlap in time, which every consumer then has to handle.
- **Reject, and make the caller buffer.** Push lateness entirely above the
  boundary, as dendro already does for schema and for seal policy. Consistent
  with the design, and the honest answer may be that a database wanting backfill
  should own that buffer.

The third is the one most in keeping with what this crate already is: it knows
about storage, and the caller knows what a row means and when it arrives.

## Outcome

Open. Nothing is implemented, and the current behavior is documented in the
README as a limitation rather than left to be discovered.

## Deferred or Reopen Items

- **(1) is not blocked on (2).** Making the drop loud is worth doing on its own
  and does not commit to any answer for backfill.
- **Reopen (2)** when a caller needs late samples and can say how late, in
  seconds. The bound is what makes the first design option tractable and the
  question answerable at all.
- Related: [segment compaction](2026-09-11-segment-compaction.md) is the other
  gap between dendro and a time-series database's storage layer.

## Appendix: Skills Invoked

- `engineering-journal` — this entry, and the index beside it.
