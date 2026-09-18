# The replication wire format

What a dendro replication stream *is*: the preamble, the framing, and the
layout of every frame. It is written so that a publisher or a subscriber could
be built from it without this crate. Where the crate is the ground truth for a
detail, the path is cited; when the two disagree, the code is what ships and
this document has a bug.

`FORMAT.md` specifies what a dendro *file* is. This is not that file — it is
the same contents in flight — and the two version independently. A frame
carries no schema version, and a change here does not bump the archive's.

Protocol version: **1** (`src/replicate/wire.rs`, `PROTOCOL_VERSION`).

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
stops *inside* a frame lost that frame; a reader reports that as an error, not
as the end (`FrameReader::next_frame`).

## 2. Framing

```
stream  := magic | version | frame*
magic   := "dendro-repl\0"                   12 bytes
version := u16                               the protocol version
frame   := u32 len | u8 kind | payload       len covers kind + payload
```

Detection is by the magic, checked before any frame is read, so a reader
pointed at the wrong socket says so rather than reporting a malformed frame. A
version this build does not implement is refused by number.

`len` is the byte count of everything after it in the frame, the kind byte
included. It exists so a reader can bound an allocation before making it and
so an unknown kind can be skipped rather than misread.

**A length above `MAX_FRAME_BYTES` (64 MiB) is refused before anything is
allocated for it.** A four-byte length read off a wire is otherwise an
allocation request of up to 4 GiB, which is an out-of-memory rather than an
error. The limit is eight times the default `SealPolicy::max_bytes`, so the
largest frame a caller on the default seal policy can produce — a `Segment` —
has ample headroom.

**A payload with bytes left over after its fields are read is refused.** The
sender put something there this build does not read, and applying the fields it
did understand would be acting on a frame whose meaning it has guessed.

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
has a completeness check rather than a guess.

| `kind` | frame | lands in |
|---|---|---|
| 1 | `Handshake` | `sources` |
| 2 | `Index` | `caller_rows` |
| 3 | `Rows` | `wal` |
| 4 | `Segment` | `segments` |
| 5 | `ClockOffset` | `clock_offsets` |

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

`source` is unique within one connection and is reassigned from zero on a
reconnect. It is an ordinal, not an identity — the uuid is the identity.

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

`seq` is **strictly increasing, one value per interval, per source per
connection.** Consecutive values mean consecutive intervals, so a jump means an
interval produced no frame.

It need not start at zero, and a frame counter is the weaker choice: a counter
increments by one whether or not an interval was skipped, so a skipped interval
arrives as a contiguous sequence with an undetectable hole in it. An **interval
index** — the observation's timestamp divided by the interval — answers the
question a subscriber is actually asking, and satisfies the same check.

Derive it from the row timestamp rather than the wall clock. `ts` is anchored
(`FORMAT.md` §5) and strictly increasing through a wall-clock step; an index
taken from the wall clock inherits the step and can go backwards.

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

One drift observation (`FORMAT.md` §5). Cheap to carry — a handful of rows per
seal — and part of the source's identity.

## 5. Protocol rules

1. A `Handshake` identifies a source and assigns the ordinal its later frames
   carry. A frame naming an ordinal with no handshake is refused.
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
6. Every interval produces a `Rows` frame, empty when nothing was observed, so
   a gap in `seq` means a lost reading and nothing else. The empty frame is
   also the keepalive.
7. `Index` is re-emitted `Full` periodically, so retention cannot orphan it.
   `caller_rows` is evicted on the same cutoff as segments, so state written
   once at the start of a recording would be deleted while later rows still
   referenced it; re-emitting at the segment-seal cadence bounds that.
8. A reconnect is a new handshake and full state. There is no resume token.
9. A row carries the `index_state` it was built against, and a subscriber that
   cannot match it **skips the row**. Misattribution is worse than a gap.

Before the first `Full` for a source, a subscriber skips rows: it has no
identity to attribute them to. Rule 2 makes that free on a clean connect, and
it bounds a mid-stream join to one `Full`.

## 6. What is not here

**Transport.** A publisher yields frames and a subscriber accepts them. HTTP, a
Unix socket, or a file is the caller's problem, and keeping it out is what
leaves dendro storage-shaped.

**Acknowledgement, retry and flow control.** There is no ack, no resume token
and no negotiation. A subscriber that falls behind reconnects and takes a fresh
handshake with full state (rule 8), and catch-up is `Segment` frames rather
than a replayed log.

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
  misreading it. Adding a field to an existing frame *does* bump the version,
  because §2 refuses a payload with trailing bytes.
- **What this does not version.** The row payload, the segment's columns and
  the index blob are the caller's. `Handshake.metadata` carries the archive's
  `encoder` key (`FORMAT.md` §6), and a subscriber whose encoder disagrees is
  refused by the same check every other read path runs
  (`segment::check_encoder`).
- **The archive schema version is separate** and is not carried here. A
  publisher and a subscriber on different archive schema versions can still
  speak protocol 1; what each can store is decided by its own file.
