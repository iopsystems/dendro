---
status: open
opened: 2026-11-13
updated: 2026-11-13
---

# Subscriber resume: continuing a copy across a reconnect

## Goal

A replication subscriber that reconnects, or restarts on the same archive,
continues its copy of each source:

- The copy holds one source row per uuid and no row twice.
- After the connection, the copy holds every row the origin holds past the
  point the two agree on. It also holds every row it held at or below that
  point, except live rows its own encoder refuses to seal, which are deleted
  and counted.
- A row the copy deletes past that point is counted. It is either shipped
  again by the origin or one the origin removed itself, by a seal's prune or
  a discard.
- A span the origin has evicted before the copy received it is reported as
  a span.

NO-GO: any test in the matrix below where the copy loses a row with no
counter moved. Also a no-go: a resume of a copy that has never sealed on its
own ships more segment bytes and rows than lie past the stream's sync
point (below). Clock offsets, handshakes and `SourceUpdate` frames
do not count toward that limit.

## Where this stands

A handshake whose uuid the subscriber's archive already holds is refused as
`Error::DuplicateSource`, and a reconnect catches up into a new archive.

## What was tried

Three designs were rejected before this one.

**First: mirror segments, judge by the copy's newest row.** The branch
`feat/unique-uuid-subscriber-resume` continued a source by uuid. The
publisher shipped every segment its archive sealed. The subscriber's
adopted segment replaced its live rows at or below the segment's `last_ts`.
Two adversarial reviews found five failures, each reproduced:

- a reconnect above the copy's oldest live row lost rows silently;
- a copy that sealed itself failed while connected;
- a segment over `MAX_FRAME_BYTES` wedged the publisher;
- 0.7.x subscribers broke at the first seal;
- origin compaction made every reconnect fail.

**Second: mirror segments, judge by counting rows.** The publisher declared
per stream a `from` timestamp. The subscriber decided from segment spans and
row counts whether a segment replaced, matched or cut what it held. A design
review found ordinary cases where that decision was wrong:

- Eviction on the origin while disconnected made the next segment delete the
  copy's older live rows, uncounted.
- A lost frame was undetectable, and the next segment deleted rows the
  origin still held.
- A discard on the origin, then new rows at the discarded timestamps, gave a
  permanent refusal. Today the publisher loses those rows silently, because
  its cursor is past them.
- A single `since` for the archive is pinned at the start of the recording
  by any quiet stream, so the first resume re-ships everything.
- A merged segment spanning mirrored and unmirrored segments was never
  shipped.
- A stream evicted to nothing on the origin caused a permanent refusal.

**Third: positions, predecessor links, and four segment cases on the copy**
(the version after that, with the publisher deciding, is this entry; its
review is answered under "Review of the publisher-decides version").
This added a resume request and protocol version 2, and fixed most of the
above. A second design review found that the copy still decided what to
delete from the segment spans it was sent:

- A segment with no predecessor came from eviction, from a stream's first
  seal, or from a discard before the first seal. The copy could not tell
  which, and treated all three as eviction. After a discard it then tried to
  seal rows its encoder had already failed on, and stopped for good.
- A merge of the origin's oldest segments has no predecessor, so it was
  inserted beside the copy's segments and every row was held twice.
- The row-count check refused merges spanning rows that the origin's seal
  had pruned.
- A `Rows` predecessor defined as the origin's previous row made every
  discard a permanent refusal.

All three missed that a reconnect re-sends index entries, and that
`caller_rows` has no primary key (`src/archive/sqlite.rs:4005`). A resumed
copy would hold each re-sent entry twice.

## What streaming replication protocols do

All three rejected designs had the subscriber reconstruct the origin's
operations (eviction, discard, compaction, a lost frame) from what it was
sent. The protocols below avoid that with four mechanisms that apply here.
The PostgreSQL and Litestream items were read from their current
documentation. The Raft and Kafka items are from memory; the second review
confirmed them.

1. **The subscriber states its position, and the server continues from
   exactly there or refuses.** PostgreSQL's `START_REPLICATION` takes a WAL
   location from the client. The server "can reply with an error, for
   example if the requested section of WAL has already been recycled".
   Kafka's fetch names an offset per partition, and an offset the broker no
   longer has is `OffsetOutOfRange`.
2. **Progress is recorded crash-safe with the data applied.** PostgreSQL
   replication origins set the remote LSN per applied transaction
   (`pg_replication_origin_xact_setup`): "if that's done replication
   progress will persist in a crash safe manner". The documentation says a
   progress row updated per transaction has "run-time overhead and database
   bloat".
3. **Each unit names its predecessor, so a gap is detected by a comparison.**
   Raft's `AppendEntries` carries `prevLogIndex` and `prevLogTerm`, and a
   follower without a matching entry rejects it. Litestream v0.5 numbers
   transactions contiguously, so it "can verify that no transactions are
   missing".
4. **When the needed history is gone, a snapshot replaces a prefix, and it
   may be sent in pieces.** Raft's `InstallSnapshot` is sent when the leader
   has discarded the entries a follower needs, in chunks with an offset.

In each, the side that holds the record of what changed chooses what the
other applies, and the other side compares before applying. In dendro the
publisher can hold both records. The resume request carries the copy's
state, and while connected the publisher records what it has shipped. So
the publisher chooses what the copy deletes, and the copy checks that the
frame's predecessor matches its own before applying it.

## Design

### Step 1: origin invariants

**A stream's timestamps strictly increase for the life of its source.** The
writer already drops a row at or below its stream's newest row (#69). The
newest row can still move backwards, and three paths cause it:

- the writer's retention pass, which re-reads the bound from the file
  (`src/writer.rs:2204`);
- the writer's discard, which does the same (`:2229`);
- `ArchiveMut::evict_before` and `ArchiveMut::discard_live_rows`, which run
  without a writer (`src/archive/sqlite.rs:2913`, `:3000`).

After any of them the origin accepts rows at timestamps it may already have
published. A running publisher does not ship them, because its cursor is past
them. That loss happens today.

The fix records a per-file bound.

- **What it is.** For each `(source, stream)` and each `(source,
  caller-row name)`, the newest timestamp this file has held, kept only when
  a delete removed it. It describes the file, not the source, for the
  reason `writer_sessions` does (`src/archive/sqlite.rs:3214-3221`): a
  copy's history of deletes is not its original's.
- **Where it lives.** In a new table, `bounds(source_id, kind, name, ts)`,
  keyed by `(source_id, kind, name)`, where `kind` separates streams from
  caller-row names. That keeps stream `cpu` and caller-row name `cpu`
  apart. Not in `sources.metadata`, where `update_source_metadata` and
  `patch_source_metadata` (`:3248`, `:3202`) would let a caller overwrite
  or drop it, and where `SourceUpdate` would carry it to copies.
- **No schema bump.** FORMAT.md §8 bumps for a new table only when its loss
  to an old copier loses data nobody can regenerate. An old copier that
  drops `bounds` produces a copy whose writer can again accept rows at old
  timestamps, which is today's behaviour. The table is created at open
  when absent, so existing v5 files get it. That needs an owner decision
  (below).
- **Who writes it.** Every public delete path that can lower a stream's
  newest held timestamp records the old one, in the same transaction:
  - retention: `evict_before`, `evict_before_with_floor`,
    `evict_streams_before`, `evict_streams_before_with_floor`;
  - `discard_live_rows`;
  - `prune_wal` (`:2880`) when it removes unsealed rows;
  - a `Sync` on a copy.

  `Transaction::delete_segment` does not write it: compaction uses it and
  never lowers the newest row (`src/rewrite.rs:347`). A ranged
  `copy_sources_into` records the newest row of what it left out.
- **Who reads it.**
  - The writer's bound becomes the larger of the table and the file: at
    open, in `refresh_newest`, and in `resume_source`'s floor
    (`src/writer.rs:1749`).
  - The resume entry point for a replicated source resets that source's
    in-memory bound to the file's newest. From then on the protocol sets
    the bound: a `Sync` or the fast path sets it to `X`.
  - A publisher uses the table in the ahead-of-origin check.
- **Mixed versions.** A writer from 0.7.x or earlier neither reads nor
  writes the table. If such a writer resumes a source after a discard, it
  can write rows at or below the bound, and a publisher's cursor past them
  does not ship them. That is today's loss, limited to this mixed-version
  case, and the same class of exception FORMAT.md §8 lists for
  `heartbeat`.

What a caller gives up: writing corrected rows at the same timestamps after a
discard. Discard exists for live rows the encoder cannot seal
(`src/archive/sqlite.rs:2995`), not for re-ingest, but I don't know whether
any caller re-ingests. The docs saying that rows written after a discard are
live rows like any others, and that the discard is not replicated, change
with this.

**Caller rows append in `ts` order.** The writer drops and counts a caller
row whose `ts` is below its name's newest. Rows at the newest `ts` are
accepted, so several can share one. After retention removes a name's newest
rows (a discard keeps caller rows, `src/archive/sqlite.rs:2992`), the next
row must be strictly above them, and `bounds` records that per name. A caller row's position is then `(ts, k)`: its
`ts` and its ordinal among the rows at that `ts`. On rezolus main
(a58d5ab2), only a test and a benchmark write caller rows, and its reader
ignores them (`crates/rez/src/reader.rs:3314`, `caller_rows_ignored`). So
the rule constrains no rezolus production path. Other callers are unknown.

**The publisher's clock-offset cursor is fixed.** Offsets are not inserted in
`ts` order: a seal records the newest sealed row of its batch
(`src/writer.rs:2683`), so sealing stream A to 100 and then B to 80 inserts
80 after 100. The publisher skips `ts <= clock_after`
(`src/replicate/publisher.rs:508`), so it never ships the 80. That is a v1
bug today, and the fix goes in step 1. `clock_offsets` has no insertion
order to resume from (`src/archive/sqlite.rs:3996-4000`). So the publisher
records the timestamps it has sent per source, and ships any it has not.
That changes v1 output: a v1 stream now carries the offsets it used to
miss. The subscriber's `finish` takes the newest offset by `ts`, not the
last one received (`src/replicate/subscriber.rs:422`).

### Whether `caller_rows` needs a primary key

No. A key on the row's content cannot work: two equal entries at one `ts`
are legal (FORMAT.md §3.5), so `(ts, blob)` is not unique. `rowid` is local
to each archive. A key the copy could share with the origin would be the
origin's position, `(ts, k)`. With appends in `ts` order the copy can
compute that position from what it holds, the publisher starts past it, and
re-sent entries are never written.

A key would also need a column that copiers carry. The key does not fix the
eviction case above; the strict bound after eviction does. Add a key only if
a second use for one appears.

### Step 2: the resume request and protocol version 2

`Subscriber::resume(writer) -> Result<(Subscriber, ResumeRequest)>`.
`ResumeRequest` is `#[non_exhaustive]`. Its byte encoding is documented in
WIRE.md, and the caller's transport carries it to the publisher side, as it
carries `since` today. For each source the archive holds, by uuid, it lists:

- per stream: the segment watermark `W`, the newest row `P`, and the copy's
  segment spans `(first_ts, last_ts)` newest first, up to a cap set during
  building;
- per caller-row name: its position `(ts, k)`;
- `since_for_new_sources`, defaulting to `i64::MIN`, for sources the request
  does not name.

A stream or name the request omits starts from `i64::MIN`. Everything in the
request is computed from what the archive holds, so it commits with the rows
it describes. A kill at any point leaves a request that matches the file.
That is mechanism 2, with no progress table.

`ArchivePublisher::resuming(db, &request)` opens a protocol version 2 stream.
Version 1 and the existing constructors are unchanged, byte for byte:

- `FrameReader` refuses a version it does not implement at the preamble
  (`src/replicate/wire.rs:573`). A 0.7.x subscriber therefore refuses a v2
  stream before applying anything.
- A v2 stream into a fresh archive is a full catch-up, because the request
  is empty.
- WIRE.md §7 says adding a field to an existing frame bumps the version, so
  v2 is required rather than chosen.
- `wire::decode_payload` and `encode_frame` are public and take no version.
  They gain one, which is an API change.

The publisher refuses a source whose requested position is ahead of the
origin: a `P` above the stream's newest row and its bound, or a caller-row
position past what the origin holds. That catches an origin restored from an
older backup, and a request sent to the wrong archive when that archive is
behind the copy.

### The sync point

For each stream, the publisher picks a sync point `X`. The copy keeps what it
holds at or below `X`, and the origin's rows above `X` replace the copy's.
Three numbers from the request and the origin decide it:

- `W` and `P`, the copy's watermark and newest row;
- `O`, the origin's oldest held row of the stream, or none.

Let `L = P` when `O` is none or above `P`, and `L = O - 1` otherwise
(saturating at `i64::MIN`). A stream the copy does not hold has no `P`, and
is shipped from its first row with no span reported. `X` is
the largest value in `[L, max(L, W)]` that is inside no copy span and no
origin span. A value is inside a span `[f, l]` when `f <= X < l`. The copy
applies a `Sync` to `X` in three parts:

1. It seals its own live rows at or below `X`. When `L > W`, the range is
   `[L, L]` and these rows exist: rows the origin evicted, discarded, or
   pruned at a seal. The copy cannot tell those apart and does not need
   to. Evicted rows stay readable, and discarded and pruned rows go, as they went on the origin.
2. It deletes what is still live at or below `X`. That covers rows the
   encoder left live after the seal and every row of a batch the encoder
   refused. Every row at or below `X` that is in no segment afterwards is
   counted as `rows_unsealable`. That includes a leading run the copy's own
   seal pruned, so the count is live rows before step 1, minus the step-1
   segment's `rows`.
3. It deletes its segments and live rows above `X`, counted as
   `rows_replaced`.

The publisher then ships the origin's segments above `X` and the live rows
above `max(X, W_o)`, where `W_o` is the origin's watermark. When `O` is more
than one past `P`, the span `(P, O)` is reported: the origin may have
evicted rows there.

Why this holds:

- `X >= L`, and every evicted row is below `O`, so no evicted row lies
  above `X`. Eviction deletes only segments whose `last_ts` is below the
  cutoff.
- No copy segment contains `X`, so the copy deletes whole segments.
- No origin segment contains `X`, so the publisher ships whole segments.
- Every origin row above `X` is shipped. A copy row above `X` is either
  shipped again or one the origin removed itself.
- When `L <= W`, the search can always fall to `L`, unless a copy span
  contains `L`. That happens only for a copy whose segments are not this
  origin's (Limits).

**Fast path.** When the origin has no segment ending above the copy's
watermark (`W_o <= W`, where an absent watermark is below every value, so
two absent ones qualify), `X = P`. Nothing is deleted or sealed, both
bounds are set to `X`, and the publisher ships the live rows above `P`. The copy's live rows stay live,
because no segment above them arrives. Without this, every resume of a
stream that neither side has sealed would ship its whole live tail again.

Otherwise, for a copy fed only by this origin, `X` is the copy's watermark. It is below
the watermark only where the origin merged across it. What is shipped again
is the copy's live rows above `X` plus the merged segment.

A `Sync` is one synchronous writer operation. An encoder error in step 1
does not end the writer thread, as a seal's encoder error does today
(`src/writer.rs:2082`), and it does not fail the `Sync`. The batch is
deleted under step 2 and counted, and the `Sync` continues. A retryable error is
retried and then fails the source; it is not deferred. On success, the
writer's newest-row bound and the subscriber's per-connection `newest` for
the stream are set to `X`, so the rows shipped again are not dropped as
late (`src/writer.rs:1564`, `src/replicate/subscriber.rs:296`).

One case still loses rows the copy held, and counts them: a step-1 batch
that mixes rows the origin evicted with rows the encoder refuses. The
encoder's error covers the batch, so all of it is deleted.

### While connected

The publisher records, per stream, the copy's spans (the request's plus
every segment it ships) and a frame counter. On each poll it runs the same
procedure against that record, for the origin segments whose `last_ts` is
past the copy's watermark. The ordinary seal gives `X` equal to the
copy's watermark with nothing live at or below it. That case is sent as a
`Segment` carrying `X`. The copy refuses it if any live row of the stream
lies at or below `X`, as `adopt_segment` refuses today
(`src/writer.rs:2580-2594`). Otherwise it deletes its live rows in
`(X, l]` and inserts the segment. The deleted rows below `f` are a leading
run the origin pruned or rows the origin discarded, and are counted as
`rows_pruned`. Any
other `X` is sent as a `Sync` followed by the segments, which covers:

- a merge on the origin that crosses the copy's watermark;
- retention on the origin that evicted past it (`O - 1 > W`), where a
  `Segment` alone would delete rows the origin no longer holds.

After a connected `Sync`, the publisher moves its row cursor back to
`max(X, W_o)`, so live rows the `Sync` deleted on the copy and the origin
still holds are shipped again. A seal that outran the row cursor needs
nothing special, because the segment carries the rows the cursor missed.

**Predecessor links.**

- Each stream has a frame counter per connection, starting at zero on both
  sides. Every `Sync`, `Segment` and `Rows` frame that touches the stream
  carries the counter value it was built after, and raises it by one. A
  `Rows` frame that spans several streams carries one value per stream.
- The subscriber keeps the same counters and refuses a frame whose value
  differs. A lost frame of any kind therefore stops the source, with an
  error naming the request to resume with. A timestamp position cannot do
  this. A `Segment` ending below the copy's newest row moves no timestamp,
  so losing it went undetected in the review of the previous revision.
- The counters are computed from the frames alone, so both sides agree,
  multi-part `Rows` frames included.
- A discard on the origin does not break the links, because a counter never
  names an origin row.
- `Index` frames carry `(ts, k)` and their predecessor in the same way. On a
  resumed name the subscriber starts with `seen_full` set when the copy
  holds entries for the source, so a first new entry sent as `Delta` does
  not make it skip rows (`src/replicate/subscriber.rs:238`, `:278`). An
  index-state mismatch stops the source on v2.

**Drops on the subscriber's writer.** The writer drops a tick after its
retries, or on a constraint error, and continues (`src/writer.rs:1635-1642`).
The subscriber's frame counter and per-connection `newest` have already moved past those rows by then. On v2, any
drop for a replicated source poisons that source: the writer refuses its
later rows, segments, `Sync` operations and index entries, and the
subscriber returns an error at its next call. The resume request is computed
from the file, so it sits below the hole, and resume ships it again.

**Segments in chunks.** A `Segment` is a header frame followed by chunk
frames with offsets, so no segment exceeds `MAX_FRAME_BYTES`. A kill leaves
the copy as it was before the frame being applied. A catch-up spans many
frames, so a kill partway leaves it partway, and resume continues from what
the file holds.

**Clock offsets** on resume are re-sent from the smallest `X` across the
source's streams. `INSERT OR IGNORE` makes a repeat harmless.

**`Subscriber::seal`** is refused on a v2 connection. The copy seals its own
rows only in step 1 of a `Sync`.

**Ahead of the origin.** The publisher refuses a source whose requested
position is ahead of what the origin holds. For a stream that means a `P`
above both its newest row and its entry in `bounds`. For a caller-row name
it means a position above both its newest entry and its entry in `bounds`. A
`Sync` that deletes a stream's newest rows on a copy records that in the
copy's `bounds`, as a discard does. So a copy of the copy, resuming from it
after a crash partway through a catch-up, is not refused as ahead of it.
The copy receives rows below that bound again from its own origin. Its
writer accepts them, because a replicated source's bound comes from the
protocol, not from `bounds`.

### Resumed sources

- A resumed source is continued through a new `Writer` entry point. It
  records no writer session, no `events` entry and no floor. The copy's
  metadata follows the origin's, through the handshake and `SourceUpdate`.
  `bounds` is per file and is not replicated.
- `complete` follows the origin. A handshake that says incomplete
  un-finalizes a finalized copy, as the origin's `resume_source` does.

### Limits

- **Copies that drew their own boundaries.** A copy whose subscriber sealed
  it on a v1 connection, or that was compacted, has segment boundaries that
  are not the origin's. The search then goes further back for an `X`. It
  ships more again, and it refuses the stream if a copy span contains `L`
  or no `X` lies within the spans the request carried. It never loses a
  row. The matrix tests both outcomes.
- **Other archives with the same uuid.** Resume is from the archive the copy
  was made from. Resuming from another archive that holds the uuid, a
  rewritten copy for example, is not analysed. The ahead-of-origin refusal
  and the predecessor links refuse what they detect. I have not shown that
  they detect every unsafe case.
- **Snapshot or memory.** The opening batch reads under one snapshot today
  (`ArchivePublisher::open`, inside `read_snapshot`). Building it as an iterator either holds the snapshot for
  the whole catch-up, which blocks WAL checkpoints, or re-reads a segment
  that compaction may have deleted in between. The choice is made during
  building.

## Review of the publisher-decides version

Two design reviews ran on this version. The first found thirteen problems.
Each is answered above:

| finding | disposition |
|---|---|
| no valid `X` for a copy with no watermark, or when the origin evicted past it | the rule now uses `L` and `[L, max(L, W)]` |
| a merge across the watermark lost when the copy had no live rows | the fast path requires `W_o <= W`; otherwise the general rule |
| a connected `Segment` deleted rows the origin evicted | a connected seal with `O - 1 > W` goes as a `Sync` |
| the copy's watermark after a self-seal is unknown to the publisher | links compare per-stream frame counters, not the watermark |
| re-shipped rows dropped by the copy's bounds | a `Sync` and the fast path set both bounds to `X`; a replicated writer's bound comes from the protocol |
| a first `Delta` after resume made the copy skip rows | `seen_full` seeded from the archive |
| a caller-row name evicted to nothing made the source "ahead" | the refusal checks the name's bound too |
| encoder errors on a seal end the writer thread | a `Sync` returns them; the mixed-batch loss is counted and stated |
| a retryable failure deferred the self-seal | retried, then the source fails |
| a chain refused after a crash partway | a `Sync` that deletes a stream's newest rows records it in `bounds` |
| the copy's bound: kept or replaced | `bounds` is per file, in its own table, not replicated |
| a 0.7.x writer on the origin after a discard | a stated mixed-version exception |
| the dropped-tick rule covered one kind of drop | any drop poisons the replicated source |

The second found no case where `X` is wrong, and ten problems around it:
- a timestamp predecessor missed a lost `Segment` (now frame counters);
- the bound's storage and meaning (now the `bounds` table);
- the bound at open on a replicated source (reset by the resume entry
  point);
- the publisher's row cursor after a connected `Sync`;
- two rules for an encoder error in step 1 (now one);
- uncounted deletions (now `rows_pruned` and the step-1 count);
- delete paths missing from the list;
- the clock-offset fix against the v1 promise (v1 output changes);
- edge cases of absent values.

## Owner decision before step 1

The `bounds` table is a format change. It adds a table to v5 files without
a schema bump, by the §8 rule that only data nobody can regenerate forces
one. The alternatives:
- a reserved metadata key, which callers can overwrite and which replicates
  to copies;
- a schema bump to v6, which per §8 means a reader for v5, a copy-forward,
  and refusing v6 in 0.7.x.

## Order of work

1. Origin invariants: strictly increasing timestamps with `bounds`, caller
   rows in `ts` order, and the clock-offset cursor fix. This lands on its
   own and fixes two losses that happen today.
2. Wire v2: frame layouts, chunked segments, the `ResumeRequest` encoding
   and the WIRE.md text, with round-trip and refusal tests.
3. Resume end to end: the publisher's `resuming` with the sync point and the
   tracked copy state, the subscriber's `resume` and v2 apply, and the writer
   entry point, the `Sync` and replace operations, and the dropped-tick rule.
   Built to the matrix below.

Each step is its own PR and review. Together they are larger than #69 to
#73. The size is forced: the position has to hold in the writer, on the wire
and at both ends.

## Test matrix

Each case runs through a real `ArchivePublisher` and `Subscriber`. Step 3
passes every case before review. "Holds" means compared row by row with the
origin, not by count.

| case | expected |
|---|---|
| resume, neither side sealed since the copy's watermark | fast path; ships only rows past `P`; no row twice; `verify` clean |
| resume, one quiet stream and one busy one | the quiet stream ships nothing |
| origin seals rows the copy holds live, connected | `Segment` carrying `X = W`; rows past `l` stay live |
| origin seals rows the publisher had not polled | the segment carries them; no error |
| origin seals while disconnected | `X = W`; ships the new segments and the live rows above them |
| first-ever origin seal while the copy is disconnected (copy has no watermark) | `X = O - 1`; the copy's live rows replaced; no refusal |
| origin seal drops leading rows the copy holds, then evicts the segment below | `L > W`; the copy seals or counts them |
| discard before the first seal, then new rows and a seal | the copy's discarded rows counted unsealable; the writer keeps running; no permanent stop |
| origin evicts past the copy's newest while disconnected | `L = P`; span reported; the copy's rows stay readable |
| origin evicts past the copy's watermark while connected, then seals | `Sync`, not `Segment`; the copy's rows stay readable |
| origin evicts a stream to nothing | no refusal |
| a step-1 batch mixing evicted rows and rows the encoder refuses | all deleted and counted unsealable |
| a retryable failure during step 1 | retried, then the source fails; nothing deferred |
| origin discards, then writes; connected and resumed | rows at or below the discarded newest dropped and counted on the origin; links hold |
| caller row older than its name's newest, on the origin | dropped and counted |
| caller rows evicted to nothing, then a row at the old `ts` | dropped and counted |
| origin merges its oldest segments | no row twice |
| copy has no live rows, origin merges across its watermark | `X` below the merge; rows past the watermark arrive |
| origin leaves trailing rows live at or below `P`, resume with `X = W` | the re-shipped rows are not dropped as late or repeated |
| origin merges across the copy's watermark, connected and resumed | `Sync`, then the merged segment |
| copy compacted, then the origin merges across the copy's merge | the search goes back; ships more; no loss |
| copy sealed by a v1 subscriber, then resumed | `X` found, or the stream refused naming why |
| a new index entry after resume | rows after it applied, not skipped |
| a caller-row name evicted to nothing on the origin | not refused as ahead |
| chain A to B to C, kill B partway through a catch-up, C resumes from B | C not refused; C completes once B does |
| a `Rows`, `Index`, `Segment` or chunk frame dropped in transit | the source stops naming the request; resume completes the copy |
| the subscriber's writer drops a tick | the source stops; resume re-ships the hole |
| segment larger than `MAX_FRAME_BYTES` | sent in chunks and adopted whole |
| index entries on resume | none written twice; entries past the position arrive |
| clock offsets inserted out of `ts` order | all shipped, v1 and v2; `finish` uses the newest |
| a lost `Segment` that ends below the copy's newest row | detected by the counter; resume completes |
| connected `Sync` after origin eviction with trailing rows live | the rows above `max(X, W_o)` shipped again |
| caller-row name and stream with the same name, discard on the stream | caller rows at the stream's old newest still accepted |
| `update_source_metadata` after a discard | the bound survives |
| replicated source reopened after a `Sync` recorded a bound | rows re-shipped below the bound accepted |
| origin restored behind the copy | the source refused as ahead of the origin |
| stream created on the origin while disconnected | shipped from its first row |
| kill during a `Sync` or a replacement | old state or new state; resume completes |
| `Subscriber::seal` on v2 | refused |
| v2 stream into a v1-only reader | refused at the preamble |
| v1 publisher, any subscriber | today's frames, plus the clock offsets the old cursor missed |
| origin `resume_source` on a source the copy finalized | copy un-finalized; rows arrive |
| chained, A to B to C, then C resumes from B | C holds A's rows, each once |
| empty archive resumes | full catch-up |

## Non-goals

- Merging two copies of one source that hold different spans. A uuid names
  one source, and a repair keeps one of each (`CopySpec::keep_sources`).
- Retention on a subscriber archive (WIRE.md rule 7).
- A producer-side publisher, one not backed by an archive, speaking v2.
