# The replication wire format

What a dendro replication stream *is*: the preamble, the framing, and the
layout of every frame. It is written so that a publisher or a subscriber could
be built from it without this crate. Where the crate is the ground truth for a
detail, the path is cited; when the two disagree, the code is what ships and
this document has a bug.

`FORMAT.md` specifies what a dendro *file* is. This is not that file — it is
the same contents in flight — and the two version independently. A frame
carries no schema version, and a change here does not bump the archive's.

Protocol versions: 1 and 2 (`src/replicate/wire.rs`, `PROTOCOL_VERSION` and
`PROTOCOL_VERSION_2`). Version 2 differs in its framing (§2.1), adds four
frames that act on a resumed copy (§4.7 to §4.10), and has a resume request
(§8). Every frame of version 1 has the same payload in version 2.

## 1. The model

A **stream** is a preamble followed by frames. It carries one or more
**sources**, each introduced by a handshake that assigns it an ordinal; every
later frame names its source by that ordinal. One connection carries every
source and every kind of content.

There is one connection rather than two because two would make ordering a
distributed-systems problem, and one makes it a question about a single FIFO.
The logical split survives as frame kinds that demultiplex to `caller_rows` and
`wal`.

A stream has no end marker. It ends when the transport ends, and a stream that
stops *inside* a frame, its length prefix included, lost that frame; a reader
reports that as an error, not as the end (`FrameReader::next_frame`).

## 2. Framing

```
stream  := magic | version | frame*
magic   := "dendro-repl\0"                   12 bytes
version := u16                               the protocol version
frame   := u32 len | u8 kind | payload       len covers kind + payload
```

Detection is by the magic, checked before any frame is read, so a reader
pointed at the wrong socket says so rather than reporting a malformed frame. A
version this build does not implement is refused by number. The framing above
is version 1's; §2.1 is version 2's.

`len` is the byte count of everything after it in the frame, the kind byte
included. It exists so a reader can bound an allocation before making it and
so an unknown kind can be skipped rather than misread.

**A length above `MAX_FRAME_BYTES` (64 MiB) is refused before anything is
allocated for it.** A four-byte length read off a wire is otherwise an
allocation request of up to 4 GiB, which is an out-of-memory rather than an
error. The limit is eight times the default `SealPolicy::max_bytes`. A
publisher splits an interval's rows across several `Rows` frames to stay under
it (rule 6). On version 1 a `Segment` cannot be split, so a segment whose
frame exceeds the limit cannot be sent; segments compacted by row count can be
that large. On version 2 the limit applies to each part (§2.1).

**A payload with bytes left over after its fields are read is refused.** The
sender put something there this build does not read, and applying the fields it
did understand would be acting on a frame whose meaning it has guessed.

### 2.1 Version 2

```
stream  := magic | version | frame*          version = 2
frame   := u32 len | u64 number | u8 kind | payload
                                             len covers number + kind + payload
```

`number` is 0 for the first frame after the preamble and one more for each
frame after it. A reader refuses a frame whose number is not the next one, so
a frame lost, repeated or reordered in transit stops the stream instead of
being applied around. Frames of a kind the reader skips (§7) count too.

A frame whose payload makes `len` exceed `MAX_FRAME_BYTES` travels as a split
frame: consecutive parts, each a frame of its own with its own number and
`len` within the limit.

```
first   := u32 len | u64 number | u8 0xfe | u64 total | bytes
later   := u32 len | u64 number | u8 0xff | bytes
```

`total` is the byte count of the joined payload, kind byte included, from 1
to `MAX_SPLIT_FRAME_BYTES` (1 GiB). The payload is the first part's bytes
followed by each later part's, in order, until `total` bytes are joined; it is
then read as one frame. A reader refuses:

- a `total` outside that range;
- a frame other than a later part before the join is complete;
- a later part with no first part before it;
- parts whose bytes exceed `total`;
- a stream that ends before the join is complete;
- a joined payload whose kind is `0xfe` or `0xff`.

A reader joins the bytes as they arrive and grows its buffer to `total` plus
at most one part, so a `total` off the wire is not an allocation. Decoding
copies a segment's bytes out of the joined payload, so a reader holds up to
about twice the frame while decoding one: about 2 GiB at the limit, all of it
bytes it received. A writer holds the frame and its encoding, also about
twice the frame. A reader or writer on a 32-bit target may fail to allocate
a frame near the limit.

A segment's parquet file and its caller index are separate SQLite values, each
up to SQLite's default limit of 1,000,000,000 bytes. A segment whose two
together exceed 1 GiB cannot be sent, and a `FrameWriter` refuses it.

## 3. Primitives

All integers are little-endian and fixed-width. There is no varint: the frames
are dominated by opaque payloads, and a variable-length integer would save
bytes only on the headers.

| name | layout |
|---|---|
| `u8` | one byte |
| `u16`, `u32`, `u64` | 2, 4, 8 bytes, little-endian |
| `i64` | 8 bytes, little-endian, two's complement |
| `bool` | `u8`, exactly `0` or `1`; any other value is refused |
| `bytes` | `u32 len` then `len` raw bytes |
| `str` | `bytes`, valid UTF-8 |
| `opt<bytes>` | `u8` presence (`0` or `1`), then `bytes` when present |
| `map` | `u32 count` then `count` × (`str` key, `str` value), **in key order** |

`opt<bytes>` distinguishes **absent** from **empty**: a segment with a
zero-length caller index is not a segment with no caller index.

A `map` is written in key order so two encodings of equal maps are equal
byte strings, which is what lets a test compare frames by their bytes.

Every timestamp is an `i64`, matching the archive (`FORMAT.md` §3): SQLite's
only integer type, and the reason a negative value means before 1970.

## 4. Frames

One kind per table in the set `rewrite` copies (`src/rewrite.rs`, `COPIED`) —
`sources`, `caller_rows`, `wal`, `segments`, `clock_offsets` — so the frame set
has a completeness check rather than a guess, plus `SourceUpdate` for a change
to a source's row in `sources` after its handshake. Version 2's `Sync`,
`SegmentAfter`, `Unavailable` and `SegmentGroup` act on what a resumed copy
holds and are outside this mapping.

| `kind` | frame | lands in |
|---|---|---|
| 1 | `Handshake` | `sources` |
| 2 | `Index` | `caller_rows` |
| 3 | `Rows` | `wal` |
| 4 | `Segment` | `segments` |
| 5 | `ClockOffset` | `clock_offsets` |
| 6 | `SourceUpdate` | `sources` |
| 9 | `Unavailable` | nothing (version 2) |
| 10 | `SegmentGroup` | nothing (version 2) |
| 135 (`0x87`) | `Sync` | `segments`, `wal` (version 2) |
| 136 (`0x88`) | `SegmentAfter` | `segments`, `wal` (version 2) |

The high bit of `kind` (`0x80`) marks a kind a reader must understand to apply
the stream correctly; see §7. `Sync` and `SegmentAfter` set it, and so do the
kinds of a split frame's parts (`0xfe`, `0xff`, §2.1). A version 1 stream that
carries `Sync` or `SegmentAfter` is refused; one that carries `Unavailable`
or `SegmentGroup` has it skipped, as an unknown kind.

### 4.1 `Handshake` (kind 1)

```
u32          source                 the ordinal later frames use
opt<bytes>   uuid                   UTF-8, canonical 8-4-4-4-12 lowercase
map          labels
map          metadata
i64          clock_anchor_wall_ns
bool         complete
```

Applied with `Transaction::insert_source_with_uuid`, so the subscriber's copy
**is** the publisher's source rather than another one with the same labels
(`FORMAT.md` §3.1). An absent `uuid` is a source from an archive written before
the column existed; the subscriber mints its own, which claims only that the
two are not known to differ.

`source` is unique within one connection, and a subscriber refuses a second
handshake for it (rule 1). It is reassigned from zero on a reconnect. It is an
ordinal, not an identity — the uuid is the identity.

### 4.2 `Index` (kind 2)

```
u32          source
str          stream
i64          ts
u8           kind                   0 = Full, 1 = Delta
u64          state.0
u64          state.1
bytes        blob
```

**`blob` is opaque and stays opaque.** It lands in `caller_rows`, which
`FORMAT.md` §3.5 says the archive never decodes, and the same rule holds on the
wire. What identifies a slot differs per caller — a CPU, a cgroup path, a
device id — so the shape of an entry belongs to the caller, not here.

`state` is the publisher's hash of the **complete** slot set after this entry is
applied, not of the entry itself. It travels outside the blob precisely so that
a subscriber can order rows against entries without decoding anything.
Hashing the result rather than the delta means a consumer applies an entry and
compares its accumulated set with one comparison, and a missed `Delta` is loud
rather than silent. It is 128 bits because a 64-bit hash over a few thousand
slots collides often enough to matter when a mismatch silently drops rows.

`Full` carries every live slot; `Delta` carries what changed, and is
meaningless without the `Full` before it.

A producer must send a stream's `Index` frames in non-decreasing `ts`. A
`Subscriber` writes them through its writer, which drops an entry below the
stream's newest and counts it (`SourceWriter::dropped_caller_rows_out_of_order`);
its `index_state` still takes the frame's `state`. `ArchivePublisher` sends
them in that order.

### 4.3 `Rows` (kind 3)

```
u32          source
u64          seq
u64          index_state.0
u64          index_state.1
u32          count
count × {
  str        stream
  i64        ts
  i64        wall_offset
  bytes      row                    opaque, the encoder's
}
```

Each row carries its own stream name, so one frame may span a source's streams.

`seq` is a **counter over intervals, not over frames.** The producer advances
it once for every interval it was meant to serve, whether or not a frame went
out. It is not a timestamp and is not derived from one. It need not start at
zero — only the step between consecutive frames is ever compared. An interval
whose rows exceed `MAX_FRAME_BYTES` goes out as several frames with the same
`seq`; the step between them is zero, which is not a gap.

| the interval | frame | `seq` |
|---|---|---|
| served, something observed | non-empty, one or more | `+1`, shared |
| served, nothing observed | **empty** | `+1` |
| **not served** — the publisher would have had to buffer | none | jumps |

**A gap therefore means intervals the subscriber did not receive, and does not
say why.** Frames lost in transit and intervals a publisher declined to buffer
are both real causes; a publisher that refuses to buffer without bound rather
than slow its own sampling for a slow reader is behaving correctly. What a gap
does *not* mean is "the interval produced nothing" — that is the empty frame.

**Count intervals; do not divide a clock.** An interval index
(`timestamp / interval`) aliases on sub-interval jitter: a reading taken
slightly early lands in the previous bucket, inventing gaps that did not happen
and hiding gaps that did.

`index_state` is the state the rows were built against. A subscriber whose
accumulated state differs skips the rows; see rule 9 below.

### 4.4 `Segment` (kind 4)

```
u32          source
str          stream
u64          rows
i64          first_ts
i64          last_ts
bytes        bytes                  one parquet file, byte-identical
opt<bytes>   caller_index
```

`rows`, `first_ts` and `last_ts` describe the **segment**, never the rows that
produced it (`FORMAT.md` §3.2). `bytes` passes through unchanged; the sender's
own seal already ran the encoder contract check over it
(`segment::materialize`), and the receiver has no input rows to re-check it
against.

`caller_index` travels with the bytes it describes, and is absent where the
publisher wrote none.

This frame is what makes catch-up cheap: shipping a sealed segment costs far
less than replaying the rows that built it.

### 4.5 `ClockOffset` (kind 5)

```
u32          source
i64          ts
i64          offset_ns
```

One drift observation (`FORMAT.md` §5). Cheap to carry: one row per seal
batch, one at finalize, and any the writer's caller records. Part of the
source's identity. Offsets are not recorded in timestamp order, so a
subscriber may receive them out of order.

### 4.6 `SourceUpdate` (kind 6)

```
u32          source
i64          ts                     ns since the Unix epoch
map          labels
map          metadata
```

A source's labels and metadata, sent when either changes after the source's
handshake; the source's other fields (`uuid`, `clock_anchor_wall_ns`,
`complete`) are not carried. Both maps are whole, not the keys that changed,
and the subscriber replaces its copy's with them, so a key the publisher
removed is removed. The copy keeps its own `encoder` and `writer_sessions`
(`FORMAT.md` §6) where it has them, since its writer maintains them. A copy
whose handshake was read before the publisher's writer recorded its session
(the writer commits a new source's row and its `writer_sessions` separately)
holds only its own sessions, and a `SourceUpdate` does not add the
publisher's. `events` is replaced, so a copy's `events` may include
`writer_session` events for sessions its `writer_sessions` does not list.

`ts` is when the publisher saw the change. An archive records no time for a
metadata change, so an archive publisher sends the wall time of the poll that
read it. A subscriber does not store it.

### 4.7 `Sync` (kind 135, version 2)

```
u32          source
str          stream
i64          at                     the sync point
```

Sets one stream of a resumed copy to the sync point `at`. The copy keeps what
it holds of the stream at or below `at`, and the frames after this one replace
what it holds above it:

1. The copy seals its live rows of the stream at or below `at` with its own
   encoder.
2. It deletes the live rows at or below `at` that are still not in a segment.
3. It deletes its segments and live rows of the stream above `at`.

The publisher picks `at` so that no segment of the copy's or of its own
contains it, so the copy deletes and the publisher ships whole segments.

A `Subscriber` applies it to a source of a resumed copy
(`Subscriber::resume`) and refuses it otherwise. Step 2's deletes are counted
in `Applied::rows_unsealable`, step 3's in `Applied::rows_replaced`, and the
newest row deleted is recorded in the copy's `bounds` (`FORMAT.md` §3.7). An
encoder error in step 1 does not fail the frame: the rows it covered are
deleted under step 2. A `Sync` whose `at` is inside one of the copy's
segments is refused.

### 4.8 `SegmentAfter` (kind 136, version 2)

```
u32          source
str          stream
i64          after
u64          rows
i64          first_ts
i64          last_ts
bytes        bytes                  one parquet file, byte-identical
opt<bytes>   caller_index
```

A sealed segment that replaces the copy's live rows of its stream in
`(after, last_ts]`. The fields after `after` are a `Segment`'s (§4.4). The
copy refuses it when it holds a live row of the stream at or below `after`, or
a segment of the stream whose `last_ts` is above `after` or at or above the
offered `first_ts`; either means the copy is not where the publisher put it.
It also refuses a segment with no rows, a `first_ts` above its `last_ts`, or
a `last_ts` at or below `after`. The live rows it deletes below
`first_ts` are rows the publisher's seal pruned or discarded, counted in
`Applied::rows_pruned`.

A `Subscriber` applies it to a source of a resumed copy and refuses it
otherwise.

### 4.9 `Unavailable` (kind 9, version 2)

```
u32          source
str          stream
i64          after                  the copy's newest row of the stream
i64          before                 the publisher's oldest held row of it
```

The publisher holds no row of the stream in the open interval
`(after, before)`, and the copy may lack rows the publisher once held there,
because retention removed them before the copy received them. Applying it
changes nothing in the copy, so the kind is skippable, and a subscriber that
skips it holds the same rows. A `Subscriber` counts it in
`Applied::unavailable`.

### 4.10 `SegmentGroup` (kind 10, version 2)

```
u32          source
str          stream
u32          count                  SegmentAfter frames that follow
```

The next `count` frames of the source are `SegmentAfter` frames of the
stream, which the copy adopts together, in one transaction, up to the size
limit below. A row of a
stream may decode only with an earlier row of it, as with an encoder that
writes a schema on the first row after a seal; when a seal of rows the copy
holds live arrives in several segments, adopting the first alone deletes
that earlier row while the copy still holds later rows live, and a
connection lost before the rest arrive leaves those rows undecodable.

The publisher sends one before the `SegmentAfter` frames of §8.2 when no
`Sync` precedes them and two or more of them have a `first_ts` at or below
the copy's newest row; `count` is the number of those, and the frames after
them are adopted one at a time.

A `Subscriber` applies it to a source of a resumed copy and refuses it
otherwise. Any other frame of the source before the group completes, a
`SegmentGroup` included, is refused, and the source stops. A group still
incomplete when the connection ends makes `Subscriber::finish` return an
error for its source.

Up to `MAX_STAGED_GROUP_BYTES` (256 MiB) of the group's segments, the
subscriber holds the group's frames in memory and counts nothing for them
until the last arrives, whose `Applied` counts all `count` segments and
their `rows_pruned`. Each frame is checked as §4.8 says against the copy as
the frames before it leave it, and one that fails commits none of them. A
group still incomplete when the connection ends is not adopted: the copy
keeps what it held before the group, and a resume continues from it
(§8.2).

When the group's segments and caller indexes pass that size, the
subscriber adopts the frames it holds, together, and each later frame of
the group as it arrives, and logs a warning. The guarantees of the previous
paragraph then do not hold for the group. A connection lost before its last
frame can leave the copy's later live rows without the row they decode
with. A resume sends the group's later segments if the publisher still
holds them; otherwise its `Sync` deletes those rows as unsealable.

The kind is skippable. A subscriber that skips it adopts each `SegmentAfter`
on its own, which is valid, and a connection lost between them can leave
the copy's later live rows undecodable, as above.

## 5. Protocol rules

1. A `Handshake` identifies a source and assigns the ordinal its later frames
   carry. A frame naming an ordinal with no handshake is refused. A
   `Handshake` may arrive at any point in the stream, for a source the
   publisher has just seen. An ordinal names one source for the life of a
   connection: an `ArchivePublisher` assigns each new source the next one,
   and a `Subscriber` refuses a `Handshake` for an ordinal an earlier one
   assigned.
2. The first `Index` frame carries complete current state (`Full`), including
   streams that exist but have never been observed.
3. The first `Rows` frame carries the latest observation of every stream. **Not
   a consistent cut**: the observations are at different times, each with its
   own window.
4. Within an interval, `Index` precedes `Rows`, so a row can never reference
   identity the subscriber has not received.
5. A row is interpreted against index entries at or before its timestamp. Time
   is the ordering axis; both carry timestamps and `caller_rows` is already
   time-keyed.
6. Every interval the publisher **serves** produces a `Rows` frame, empty when
   nothing was observed; the empty frame is also the keepalive. An interval
   whose rows exceed `MAX_FRAME_BYTES` produces several `Rows` frames with the
   same `seq`. `seq` counts intervals rather than frames, so an interval the
   publisher could not serve leaves a gap. A gap means intervals the subscriber
   did not receive, whether lost in transit or never sent.
7. `Index` is re-emitted `Full` periodically, so retention cannot orphan it.
   `caller_rows` is evicted on the same cutoff as segments by default, so
   state written once at the start of a recording would be deleted while
   later rows still referenced it; re-emitting at the segment-seal cadence
   bounds that. Re-emission does not make retention on the receiving side
   safe by itself: a cutoff can fall between a `Full` and the deltas after
   it. An archive writer that knows which rows are `Full` can keep them with
   a floor (FORMAT.md §3.5). This crate's `Subscriber` does not record an
   entry's kind and has no retention of its own, so a subscriber archive has
   no retention path yet.
8. On version 1 a reconnect is a new handshake and full state, and a
   `Subscriber` refuses a handshake whose `uuid` its archive already holds
   (`Error::DuplicateSource`): a reconnect catches up into a new archive. On
   version 2 a copy resumes: `Subscriber::resume` builds the request of §8
   from the copy's archive, and a handshake for a source it names continues
   that source (§8.1).
9. A row carries the `index_state` it was built against, and a subscriber that
   cannot match it **skips the row**. Misattribution is worse than a gap.
10. A subscriber drops a row at or below the newest timestamp the connection
    has already delivered for that row's stream, and a second row at one
    `(stream, ts)` within a frame, and counts them (`Applied::rows_repeated`).
    A frame delivered twice therefore writes nothing twice, and a row sent
    after a newer row of its stream in an earlier frame is dropped the same
    way. Within a frame, a stream's rows may be in any order.

Before the first `Full` for a source, a subscriber skips rows: it has no
identity to attribute them to. Rule 2 makes that free on a clean connect, and
it bounds a mid-stream join to one `Full`.

## 6. What is not here

**Transport.** A publisher yields frames and a subscriber accepts them. HTTP, a
Unix socket, or a file is the caller's problem, and keeping it out is what
leaves dendro storage-shaped.

**Acknowledgement, retry and flow control.** There is no ack and no
negotiation: the publisher's side picks the version, and a reader that does not
implement it refuses the preamble. A subscriber that falls behind reconnects
and takes a fresh handshake with full state (rule 8), and catch-up is
`Segment` frames rather than a replayed log. The resume request (§8) is
carried by the caller's transport, not by the stream.

**Compression and encryption.** A `Segment`'s parquet is already compressed
(`segment::writer_props`); anything else belongs to the transport.

**Authentication.** Frames carry no credentials. A subscriber applies what it
is handed, so whatever decides who may hand it anything is the transport's.

## 7. Compatibility

- **What bumps the protocol version.** A change to the preamble, to the
  framing, or to the layout of an existing frame — anything a reader of the
  current version would misread silently.
- **What does not.** A new frame kind. The length prefix makes an unknown kind
  skippable, so an older subscriber degrades to not applying it rather than to
  misreading it. `FrameReader` skips one and counts it (`skipped()`). Before
  dendro 0.3.0 it returned an error instead, so a subscriber built on 0.2.x
  stops at the first kind it does not know. Adding a field to an existing
  frame *does* bump the version, because §2 refuses a payload with trailing
  bytes.
- **A kind that must not be skipped.** A new kind whose absence would make a
  reader apply later frames wrongly (one that removes or replaces what the
  subscriber holds, say) sets the high bit of `kind`. A reader that does not
  know such a kind refuses the stream there instead of skipping it, and the
  frames before it stand. `FrameReader` does this in releases after 0.7.1.
  Readers on 0.7.1 and earlier do not know the bit and skip such a frame like
  any other unknown kind, so a producer that sends one to an older subscriber gets that
  subscriber's skip, not its refusal.
- **What this does not version.** The row payload, the segment's columns and
  the index blob are the caller's. `Handshake.metadata` carries the archive's
  `encoder` key (`FORMAT.md` §6). A `Subscriber` refuses a `Handshake` whose
  `encoder` differs from the version its own encoder reports, by the rule
  every read path applies (`segment::check_encoder`): either side reporting
  no version is not a mismatch.
- **Version 2.** Its framing differs from version 1's, so it is a new
  version rather than new kinds. A reader on 0.7.1 or earlier refuses a
  version 2 stream at the preamble, before it applies anything. This build
  reads both versions; `write_preamble` and `encode_frame` write version 1,
  and a `FrameWriter` writes either.
- **The archive schema version is separate** and is not carried here. A
  publisher and a subscriber on different archive schema versions can still
  speak the same protocol version; what each can store is decided by its own
  file.

## 8. The resume request

What a copy holds of each source, sent from the subscriber's side to the
publisher's by the caller's transport before a version 2 stream starts
(`ResumeRequest::encode`, `ResumeRequest::decode`).

```
request  := magic | u16 version | i64 since_for_new_sources | u32 count | source*
magic    := "dendro-resume\0"                 14 bytes
version  := 2                                 the protocol version it asks for
source   := str uuid
            u32 count | stream*
            u32 count | name*
stream   := str stream
            i64 newest                        the copy's newest row, sealed or live
            u32 count | (i64 first_ts, i64 last_ts)*
                                              the copy's segments, newest first
name     := str name
            i64 ts                            the copy's newest entry of the name
            u64 at_ts                         how many entries it holds at ts, >= 1
```

A source is named by uuid; `since_for_new_sources` is where a source the
request does not name is shipped from, `i64::MIN` for its first row. A stream
or caller-row name a source omits is one the copy holds no row of. The
watermark of a stream is the first span's `last_ts`.

A reader refuses a request that:

- exceeds `MAX_FRAME_BYTES` or has bytes left over;
- has a wrong magic or a version other than 2;
- lists sources out of uuid order, or streams or names out of name order, or
  any of them twice;
- has a span whose `first_ts` is above its `last_ts`, or spans that overlap or
  are not newest first;
- has a `newest` below the watermark, or an `at_ts` of 0.

A caller-row name's entries append in `ts` order (`FORMAT.md` §3.5), so
`(ts, at_ts)` is a position in the publisher's entries of that name too.
`ArchivePublisher::resuming` answers a request (§8.2).

### 8.1 The copy's side

`Subscriber::resume` reads the request from the copy's archive, so after a
kill it describes what the file holds. It lists at most 256 spans per stream,
newest first, and only the newest when 256 would make the request exceed
`MAX_FRAME_BYTES`. A copy with more streams than that fits is refused by
`ResumeRequest::encode`. A source with no uuid is left out, and the publisher's
handshake for it adds a second source.

A handshake for a source the request named continues it without a writer
session. Its labels and metadata are replaced with the handshake's, as a
`SourceUpdate` replaces them. A handshake that says incomplete clears the
copy's `complete`. The connection's newest row per stream starts at the
copy's newest, so a row at or below it is a repeat (rule 10), until a `Sync`
sets it to the sync point. A copy that holds index entries for the source has
had its `Full`, so a first `Delta` is applied. Its index state starts at
`NO_INDEX_STATE`, so the publisher's index-state values on the connection
start there too: rows built against entries the copy already holds carry
`NO_INDEX_STATE`, and each entry sent changes the state from it. A handshake
for any other source adds it.

Each source of a resumed subscriber stops at the first frame for it that
fails, and at the first write its writer drops: a tick dropped after
retries, a row at or below its stream's newest, or an index entry below its
name's newest. A `Rows` frame whose index state the copy does not hold
fails, where a subscriber that resumes nothing skips its rows (rule 9). From
then on `apply`, `sync` and `finish` return an error, `finish` does not
finalize the source, and the writer writes none of the source's rows, index
entries, segments or syncs, so a new request asks for what is missing. A
resumed subscriber does not seal; its segments come from `Sync` and
`SegmentAfter`.

Frames lost before they reach `apply` are not detected by the subscriber;
a version 2 `FrameReader` refuses a stream that lost one (§2.1).

### 8.2 The publisher's side

`ArchivePublisher::resuming` refuses a request that is ahead of its archive:
a stream whose `newest` is above every row the archive has held of it, its
`bounds` included, or a stream it has never held; a caller-row position past
its entries. That is an origin restored from an older backup, or a request
sent to the wrong archive.

It handshakes every source. A source the request does not name is shipped
from `since_for_new_sources`, as a catch-up. For a named source, each
caller-row name continues after the request's position, and each stream is
handled by what the copy holds of it:

- A stream the copy does not hold ships from its first row, as `Segment`
  frames and then its live rows.
- When the archive holds no segment ending past the copy's watermark (an
  absent watermark is below every value), the copy keeps everything and the
  publisher ships the live rows past the copy's newest row, `P`.
- Otherwise the publisher picks a sync point `X`. With `O` the archive's
  oldest held row of the stream, `L` is `P` when `O` is absent or above `P`,
  and `O - 1` otherwise. `X` is the largest value in `[L, max(L, W)]`, `W`
  the copy's watermark, inside no segment of the copy's spans and no segment
  of the archive's, where a value is inside `(first, last)` when
  `first <= X < last`. The stream gets a `Sync` to `X` unless `X` is `W`,
  then the archive's segments with `last_ts` above `X`, oldest first, as
  `SegmentAfter` frames, each `after` the previous one's `last_ts`, then
  the live rows past what the copy then holds. With no `Sync`, a
  `SegmentGroup` (§4.10) precedes the `SegmentAfter` frames when two or
  more of them have a `first_ts` at or below `P`.

A stream whose `O` is more than one past `P` also gets an `Unavailable` from
`P` to `O`. A stream the copy holds and the archive holds no row of gets one
from `P` to one past the archive's `bounds` entry for it, when that entry is
above `P`; a later row of the stream reaches the copy through a sync point
as above. Clock offsets are sent again from the smallest point a stream
continues from; the copy ignores one it holds.

While tailing, the publisher keeps the copy's spans (the request's, then
every segment it ships, less those a `Sync` deleted) and the copy's newest
row of each stream. A poll that finds a segment ending past the copy's
watermark runs the same procedure, with that newest row as `P`, the
`Unavailable` included. When `X` is the copy's watermark, the segments go
out as `SegmentAfter` frames, preceded by a `SegmentGroup` when two or more
of them have a `first_ts` at or below `P`, as above; otherwise a `Sync`
precedes them. A source created after the publisher opened, and a source
the request did not name, are caught up from `since_for_new_sources`; the
copy's spans for them start as every segment the archive holds.

Limits:

- A copy whose segment boundaries are not the archive's, because a version 1
  subscriber sealed it or it was compacted, can leave no value in
  `[L, max(L, W)]` outside every span. The stream is then refused. A sync
  point below the oldest span the request lists may lie inside a segment
  the request left out, which the copy refuses (§4.7).
- Resuming from an archive other than the one the copy was made from, such as
  a rewritten copy of it, is refused only where the checks above detect it.
