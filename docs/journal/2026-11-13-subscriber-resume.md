---
status: open
opened: 2026-11-13
updated: 2026-11-13
---

# Subscriber resume: continuing a copy across a reconnect

## Goal

A replication subscriber that reconnects, or restarts on the same archive,
continues its copy of each source: one source row per uuid, no row twice,
no row lost, and no error from a copy that is only behind.

## Where this stands

A handshake whose uuid the subscriber's archive already holds is refused as
`Error::DuplicateSource`, and a reconnect catches up into a new archive.
Before that refusal, every reconnect inserted a second source with the same
uuid and copied everything again.

## The constraint

A publisher ships a sealed segment whole, as Parquet bytes, and neither side
can split one: dendro does not decode a segment except to project or compact
it, and the rows inside are the caller's encoder's. So a copy can adopt a
segment only when the segment's span does not straddle anything the copy
already holds in a segment of its own. A copy holds rows in two forms, live
WAL rows and sealed segments, and the copy's segments are cut wherever its
own seals cut them.

## What was tried

The branch `feat/unique-uuid-subscriber-resume` continued a source by uuid
(a resume with no floor or anchor check), had the publisher ship every
segment its archive sealed (rows already shipped live included), and had the
subscriber's adopted segment replace its live rows at or below the segment's
`last_ts`. Two adversarial reviews found, each reproduced:

1. **Silent loss.** A reconnect with `catching_up(since)` above the copy's
   oldest live row, or with `tailing`, shipped a segment that started above
   rows the copy held live. Adopting it shadowed and pruned them. `verify`
   reported nothing.
2. **A copy that seals itself fails while connected.** After
   `Subscriber::seal`, the origin's next seal straddles the copy's segment
   and `adopt_segment` refuses it. Sealing the subscriber was the documented
   pattern.
3. **A segment over `MAX_FRAME_BYTES` wedges the publisher** for every source
   on every poll, where before a tailing stream never shipped segments.
4. **0.7.x subscribers break** at the publisher's first seal: they refuse a
   segment over live rows. WIRE.md §7 says a kind that replaces what the
   subscriber holds sets the must-understand bit, and this reused `Segment`.
5. **Compaction on the origin** can produce a segment that straddles what the
   copy holds, so every reconnect fails (read from the code, not reproduced).

## Options

**A. Mirror the publisher's segments, with guards.** Keep the mirroring,
and:
- Ship a newly sealed segment as a new frame kind with the must-understand
  bit set. A 0.7.x subscriber skips it and keeps today's behaviour (it seals
  its own), so the mixed-version break goes.
- Adopt a mirrored segment only if no live row of the stream lies below its
  `first_ts`; otherwise refuse, naming the `since` that would cover them.
  This closes case 1 with an error instead of a loss.
- Skip a mirrored segment the copy already holds the rows of (its newest
  row at or above the segment's `last_ts`), keeping the copy's own rows and
  segments. This tolerates a copy that seals itself (case 2) and an oversized
  segment whose rows went out live (case 3), at the cost that such a copy's
  segments are not the origin's.
- A compacted segment that straddles what the copy holds is skipped by the
  same rule when the copy holds its rows, and refused otherwise (case 5).

Open question: "the copy holds the rows" is judged by the copy's newest row,
which assumes the copy's rows are contiguous. A copy that earlier skipped
rows (`rows_skipped` for an index-state mismatch) has holes this rule cannot
see.

**B. A resume position.** `Subscriber::positions()` reports, per uuid and
stream, the newest row the copy holds, the transport carries it to the
publisher, and a new `ArchivePublisher::resuming(db, positions)` ships only
what lies past it: segments wholly past the position, and live rows past it.
A segment that straddles the position (the origin sealed past what the copy
holds, case 1 and 5) still cannot be shipped without splitting it, so B
needs A's adoption rule for that segment anyway. Its gain is that it never
re-ships what the copy holds. Its cost is a back channel the wire has never
had (WIRE.md §6).

**C. Leave resume out.** Keep the refusal. A subscriber restart re-copies
into a new archive. The cost is the full copy on every reconnect.

## Recommendation

A, without B. It fixes every case above with a refusal or a skip that says
why, keeps the wire push-only, and degrades a 0.7.x subscriber to today's
behaviour. B can follow if re-shipping on reconnect is measured to matter.

## What decides it

Before building, a test matrix that the implementation must pass, each case
reached through a real `ArchivePublisher` and `Subscriber`:

| case | expected |
|---|---|
| reconnect `catching_up(MIN)`, copy never sealed | one source, origin's segments, no row twice |
| reconnect after the origin sealed rows the copy holds live | segment adopted, live rows replaced |
| reconnect `catching_up(since)` above the copy's oldest live row | refused, naming the `since` |
| reconnect `tailing` with the origin sealed while disconnected | refused, or the rows arrive; never a silent drop |
| copy sealed with `Subscriber::seal`, origin seals later | mirrored segment skipped, copy unchanged |
| origin segment over `MAX_FRAME_BYTES`, rows already shipped | skipped and logged; other sources continue |
| 0.7.x subscriber against the new publisher | today's behaviour (mirrored kind skipped) |
| origin compacts in place, then reconnect | skipped if the copy holds the rows, else refused |
| copy with an index-state gap, then reconnect | documented outcome (the open question above) |

## Non-goals

Merging two copies of one source that hold different spans. A uuid names one
source, and a repair keeps one of each (`CopySpec::keep_sources`).
