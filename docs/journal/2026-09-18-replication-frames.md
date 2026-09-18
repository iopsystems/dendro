---
status: implemented
opened: 2026-09-18
updated: 2026-09-18
issues: [3]
---

# Replication: an archive's contents as frames, and the applier that reverses it

## Goal

Make replication a dendro capability: frame types and a codec, a `Subscriber`
that applies frames to an archive, and an `ArchivePublisher` that tails one.
Transport stays outside.

The request came from rezolus, which needs to stream an agent's metrics into an
archive on another host and had been designing a wire format to do it. That
format kept converging on dendro's storage model. The question this entry
answers is whether that convergence means the format belongs here, and if so
what shape it takes without the container learning what a row means.

## Decision Criteria

**GO if the frame set has a completeness check rather than a guess.**
`rewrite.rs` already copies an archive into another one, and the fixed set of
tables it carries is this crate's own statement of what an archive is. Every
table in that set needs a frame type, or the copy is lossy and the feature is a
half-copy wearing the name of a whole one.

**GO if round-trip losslessness is testable here, with no caller involved.**
Publish an archive, subscribe into a fresh one, compare what reads back. That is
the strongest available test of a replication format, and it is untestable in
either repository alone.

**NO-GO on the container learning what an index entry says.** FORMAT.md §3.5
makes `caller_rows` opaque, and the survey's scope test is whether two callers
with completely different row shapes could both use a feature. A frame set that
defines slots and labels fails both.

**NO-GO on multi-writer or clustering.** One writer, one file, stated from the
start.

## Scope

In: the frame types and codec, the wire specification, the subscriber, the
archive publisher, and the two writer APIs the subscriber needed.

Out: transport, authentication, acknowledgement and flow control. Out: any
publisher that is not an archive — rezolus's agent synthesizes frames from live
metrics because it has no archive to tail, and that is the one piece that cannot
live here.

## Evidence

### The scope question was already answered, and answered the other way

[The TSDB survey](2026-09-12-what-a-tsdb-has-that-we-do-not.md) lists
replication under *"Missing, and not ours — contradicts a non-goal. One writer,
one file, stated from the start."* Reversing that needs an argument, not a
preference.

The argument is that the two things share a word and not a mechanism. What the
survey ruled out is a cluster: several writers, a consensus protocol, a
partitioned key space. What this builds is a serialization of an archive's
contents plus an applier. Each side still has exactly one writer and one file;
the wire carries no locks, no acknowledgements and no resume token, and a
reconnect is a fresh handshake. The non-goal survives intact, and that entry is
amended rather than left contradicting the code.

### The completeness check exists and is already enforced

`rewrite.rs` has a test whose only job is to fail when a table is added to the
schema without the copy learning about it, and it names the set:

```rust
const COPIED: &[&str] = &["sources", "segments", "wal", "clock_offsets", "caller_rows"];
```

Five tables, five frame kinds. The fifth is the one that would have been
forgotten: `clock_offsets` had no path through the writer at all, because the
writer records those itself at every seal and at finalize, so no caller had ever
needed to write one. A subscriber does, and without it the copy would be lossy
against exactly the list the crate uses to define itself.

### The issue's acceptance criterion cannot hold as written

The proposal asked for "compare the five `COPIED` tables". Reading
`copy_sources_into` shows three reasons that is not the assertion to make:

1. It materializes the live WAL tail **into a segment**; it never carries `wal`
   rows as `wal` rows. A subscriber does the opposite — rows land in its own WAL
   and it seals on its own cadence — so at any given moment the same rows sit in
   different tables on the two sides.
2. `sources.id` is local to a file and renumbered by every copy (FORMAT.md
   §3.1).
3. `segments.seq` is renumbered densely from zero by a copy.

So the test compares what **reads back**: sources by uuid, labels, anchor and
`complete`; each stream's rows spliced segments-then-tail the way §4 rule 3
splices them; the clock-offset series; and the caller store by name and
timestamp. That is the property the format is for, and the one a consumer would
notice losing.

### Three defects the implementation found

All three were found by tests rather than by review, and all three were in rules
that read correctly on paper.

**A publisher with no secondary index had every row dropped.** The subscriber
waits for a `Full` index entry before applying rows, which is what bounds a
mid-stream join. A caller that keeps no index never sends one, so its rows
waited forever for a frame that was not coming. `NO_INDEX_STATE` is now exempt:
rows built against no index are always resolvable. The cost is that a publisher
which *has* an index must never declare `NO_INDEX_STATE`, which is now stated on
the constant.

**An index that appeared after the publisher attached never sent a `Full`.**
The archive publisher marks the opening batch of index entries `Full` and
everything after it `Delta`, which is the truth for an archive: the opening
batch is everything it holds. But an archive with no index entries yet has an
*empty* opening batch, and the code marked the source as having sent its `Full`
on the strength of it. A caller that started keeping an index afterwards then
sent only `Delta`s, and the rule above — wait for a `Full` — skipped every row
for the life of the connection. Found while reviewing for the pull request, by
asking what happens when the opening batch is empty; `Full` is now keyed on
having emitted an entry rather than on a batch having finished.

**The publisher's seal detection required a watermark that need not exist.** A
tailing publisher reads the live WAL tail, so a seal in the source carries rows
out of view and a publisher polling more slowly than the source seals misses
them silently. The check first asked whether the watermark had moved *since the
last poll*, which a stream that has never sealed cannot answer — and a stream
that has never sealed is exactly the one at risk. It is `watermark > cursor`:
`live_wal` returns exactly the rows past the watermark, so a watermark at the
cursor is the ordinary case and only one past it means rows went by unread.

### What is not measured here

The numbers motivating the work were taken in rezolus and are quoted from
[issue #3](https://github.com/iopsystems/dendro/issues/3): streaming rows
instead of polling snapshots is 2.6x smaller; two frames carrying identical row
counts cost 45,350 B and 158,360 B depending on whether identity was re-sent;
one metric group with 638 members changed its schema on 59 of 60 consecutive
scrapes; a five-minute buffer at a 1 s interval carries roughly 8x more repeated
identity than values.

**This effort took no new measurements.** Nothing here has been benchmarked
against anything: not the codec, not the publisher's poll cost, not a real
transport. The tests pin behavior, not numbers.

## Design and Implementation

Four commits, behind a non-default `replicate` feature that does **not** imply
`write` — publishing is a read, so the reader-only build publishes and only the
subscriber needs a writer.

### The index entry stays opaque (`ae34fad`)

The proposal specified `IndexEntry { kind, slots, removed, state }` with
`SlotEntry { slot, labels }`. Shipping that would make dendro know what a slot
and a label are, which fails the survey's scope test and contradicts §3.5.

What dendro actually needs in order to order rows against entries is one
comparison. So the **state hash travels outside the blob**, in the frame header:

```rust
Index { source, stream, ts, kind, state: (u64, u64), blob: Vec<u8> }
Rows  { source, seq, index_state: (u64, u64), rows: Vec<WalRow> }
```

The subscriber writes `blob` to `caller_rows` verbatim and compares two `u64`
pairs. Rules 4, 5 and 9 all work, nothing is decoded, and the slot-and-label
shape stays in the caller where it belongs.

`WIRE.md` specifies the bytes, for the reason FORMAT.md is written the way it
is: a non-Rust implementation should be buildable from the document. The codec
is hand-rolled, which keeps the dependency graph where it is — the crate carries
`serde_json` and no `serde` — and keeps the format a specification rather than
whatever a deriving crate does this release.

Three limits, because the bytes come off a wire: a length above
`MAX_FRAME_BYTES` is refused **before** the buffer is grown, so a corrupt
four-byte length is an error and not a 4 GiB allocation; a payload with bytes
left over after its fields is refused, because applying the fields that were
understood is acting on a frame whose meaning was guessed; and a stream that
stops inside a frame is an error rather than an end of stream, which would
silently shorten the recording.

### Two writer APIs (`2659203`)

`Writer::add_source_with_uuid` carries an identity minted elsewhere, so the row
here **is** that source. `add_source` is now this with `None`.

`SourceWriter::adopt_segment` is the seal path's other entrance: `seal` says
"encode what is in the WAL", this says "here are the bytes". It reuses the
writer thread's existing `next_seq` and `watermarks` maps and advances them only
after the commit, the same ordering `seal_batch` uses for its retry safety.

It does not run `segment::materialize` — there are no input rows to check the
segment against, and the sender's own seal already ran that contract. What it
checks is **where the segment lands**, and the three answers are the design:

- Wholly at or below the watermark: already held, `Ok(false)`. A reconnect asks
  for a span and is re-sent segments the subscriber has, so refusing those would
  make an ordinary reconnect an error.
- Straddling the watermark: refused. The rows below it could never be read
  (FORMAT.md §3.3), and splitting the segment would mean decoding it.
- Reaching an unsealed row: refused. A seal earns the right to prune by having
  encoded exactly the rows it covers; an adopted segment proves nothing about
  the subscriber's own WAL rows. Refusing keeps `adopt_segment` a pure insert —
  it never deletes anything.

### The subscriber (`e99584d`)

Owns a `Writer`, because the subscriber's archive is written the way every other
archive is. What arrives over the wire changes what is written, not how.

**It does not seal.** dendro never seals on its own and a subscriber is not an
exception; `Subscriber::seal` lets the caller drive a `SealPolicy` exactly as a
local recording does.

Two rules decide whether a row lands, and they are one judgement: a row whose
identity cannot be resolved is dropped, because attributing it to the wrong slot
is a wrong value with nothing to show for it, where a gap is visible. `Applied`
is returned rather than logged, the way `Evicted` and `Compacted` are, so a
caller can tell "nothing arrived" from "everything was dropped" — two outcomes
that look identical from outside and mean opposite things.

`complete` travels. A copy of a finished source answers "was this finished" the
same way; a copy of a live tail stays incomplete, which is the truthful answer,
because there **is** data after its last row.

### The archive publisher (`844472f`)

Reads through the ordinary read API rather than SQLite's session extension. A
changeset captures physical row changes, prunes and evictions included, and
would make the subscriber reproduce the publisher's local `sources.id` and
`segments.seq`, neither of which is an identity. Every poll takes one
`read_snapshot`, for the reason `copy_sources_into` takes one.

Only the opening catch-up batch emits `Segment` frames. Once tailing, shipping
the publisher's segment as well would offer the subscriber a segment covering
rows it already holds unsealed, which `adopt_segment` refuses — the three checks
above turned out to constrain the publisher as much as the subscriber.

The publisher's index state is an **accumulator over the entries it has
emitted**, not a hash of anything decoded, because it republishes blobs it
cannot open. That is a weaker claim than the proposal's "hash of the complete
slot set" and it is enough for rule 9, which needs only a value that changes
with every entry sent. A producer-side publisher declares the slot-set hash
instead; both are opaque to dendro, and the subscriber compares rather than
recomputes either way.

It does not re-emit `Full` periodically (rule 7). `caller_rows` has no primary
key, so re-sending entries would duplicate them rather than replace them. Rule 7
is eviction safety for a producer-side publisher, whose subscriber can lose
state to retention; an archive publisher's subscriber keeps what it was sent.

## Outcome

Implemented. The schema version stays at **4**: no new tables, no change to the
live-WAL rule or the time model, and an adopted segment writes the same
`segments` row a seal writes.

Verified by `cargo test --all-features` and `cargo clippy --all-features
--all-targets`, with `cargo test --no-default-features --features replicate` for
the publisher without a writer, and `./scripts/check-wasm.sh` for the same
configuration on wasm32.

That last one did not exist and could not be run on macOS at all: Apple's clang
has no WebAssembly backend, and the failure arrives from inside cc-rs as a bare
exit status several hundred lines into a build of zstd and SQLite, which reads
like a broken dependency rather than a missing toolchain. So the claim that
publishing works in the reader build was untested on the machine it was written
on, and CI would have been the first to know. `scripts/check-wasm.sh` finds a
clang that has the backend — Homebrew's, on macOS — and CI now runs the script
rather than the bare cargo commands, so it cannot rot into something only one
machine can execute. `tests/replicate.rs` carries 13 cases and
`src/replicate/wire.rs` 10 codec unit tests; six new cases in
`tests/writer_policy.rs` pin the writer APIs.

`tests/replicate.rs::round_trip` is the one the effort was for: an archive with
two sources, several streams, sealed segments, a live tail, clock offsets and
caller rows — including two caller rows sharing a timestamp, which a cursor
keyed on timestamp alone resumes wrongly — published, subscribed into a fresh
archive, and compared by readable content across all five tables.
`round_trip_through_the_codec` runs the same frames through `FrameReader`, which
is the only way a real publisher and subscriber are ever connected.

What makes the entry worth keeping is the opaque-index decision. The proposal's
shape was concrete, plausible and would have worked, and taking it would have
put slots and labels in a container whose entire design rests on not knowing
what a row means. Moving one hash out of the blob costs nothing and keeps the
boundary, and that is not obvious from either side on its own.

## Deferred or Reopen Items

The proposal closed with three open questions. Two are answered here and the
third is not built; recording all three, because an issue is the task layer and
this is where the decision belongs.

- **`Publisher` is concrete, not a trait.** Answered by building it.
  `ArchivePublisher` is a struct, and a producer with no archive to tail
  constructs [`Frame`] values directly rather than implementing something. The
  frame types already *are* the interface between the two halves, and a trait
  over "yields frames" would be a second one describing the same boundary — with
  nothing on the subscriber side able to tell the implementations apart, since
  [`Subscriber::apply`] takes a `Frame` either way. Reopen if a caller turns up
  that needs to be generic over the source of its frames, which the one known
  consumer is not: its agent synthesizes frames from live metrics and hands them
  straight to the codec.
- **A subscriber cannot request a subset of streams.** Not built, and the
  proposal's suggested substrate does not apply: that idea rested on SQLite's
  session extension carrying `Changeset::apply`'s filter, and the publisher does
  not use the session extension for the reasons above. A filter would be an
  ordinary predicate instead, and `CopySpec::keep_streams` is the precedent for
  its shape — the caller's decision, because it is about what rows *mean*.
  Nothing needs it yet: an archive publisher ships what the archive holds, and a
  producer-side publisher simply does not emit what it does not want to send.
  Reopen when a subscriber needs less than a publisher is willing to send, which
  is a transport-bandwidth problem before it is a format one.
- **A tailing publisher must poll faster than the source seals.** It reads the
  live WAL tail, so a seal carries rows out of view. `next` detects a watermark
  past its cursor and returns an error naming the stream and the timestamp to
  reconnect from, rather than shipping a stream with a hole in it. The recovery
  is a reconnect with `catching_up`, which is correct but costs a handshake. A
  publisher that shipped the covering segment instead would not, and the reason
  it does not is that a segment may begin below the cursor, which is the
  straddle case `adopt_segment` refuses. Reopen if the reconnect cost turns up
  in practice.
- **There is no `Finalize` frame.** A source that finalizes while a subscriber
  is tailing leaves the copy incomplete forever, because `complete` is read once
  at the handshake. That is the truthful answer for a live tail and the wrong one
  after the publisher stops. Reopen when a caller needs a tailed copy to close
  cleanly without reconnecting.
- **The wire is unauthenticated and uncompressed**, and deliberately: both
  belong to the transport, which is also where the frames came from. Recorded so
  it is a decision rather than an omission.
- **No measurements.** Nothing here has been benchmarked. The numbers in
  Evidence are rezolus's and motivate the feature; they say nothing about what
  this implementation costs. Reopen with a real transport under it.
- Related: [the TSDB survey](2026-09-12-what-a-tsdb-has-that-we-do-not.md) owns
  the scope classification this entry amends;
  [schema churn](2026-09-13-schema-churn-and-column-identity.md) owns the
  secondary index whose transitions these `Index` frames carry.

## Appendix: Skills Invoked

- `engineering-journal` — this entry, and the index row beside it.
