//! Frames as bytes. `WIRE.md` is the specification; this is the reference
//! implementation of it.
//!
//! Hand-rolled rather than derived, for the reason `FORMAT.md` is written the
//! way it is: the byte layout is a thing a non-Rust implementation can be
//! built from, and a derived encoding is whatever the deriving crate does this
//! release. It also keeps the dependency graph where it is, which is what lets
//! the reader half build for `wasm32-unknown-unknown`.

use std::collections::BTreeMap;
use std::io::{Read, Write};

use crate::archive::{SegmentMeta, WalRow};
use crate::error::{Error, Result};

use super::frame::{Frame, IndexKind};
use super::resume::{CallerRowPosition, ResumeRequest, SourcePosition, StreamPosition};

/// Written once at the head of a stream, before any frame.
pub const MAGIC: &[u8; 12] = b"dendro-repl\0";

/// Protocol version 1: what [`write_preamble`] writes and [`encode_frame`]
/// frames. A version is bumped when the preamble, the framing or a frame's
/// layout changes in a way an older reader would misread; a new frame kind an
/// older reader can skip does not need one, because the length prefix makes
/// skipping possible.
pub const PROTOCOL_VERSION: u16 = 1;

/// Protocol version 2 (WIRE.md §2.1). Every frame carries a number, so a
/// reader detects a lost or repeated frame; a frame larger than
/// [`MAX_FRAME_BYTES`] travels in parts; and the frames that act on a
/// resumed copy ([`Frame::Sync`], [`Frame::SegmentAfter`],
/// [`Frame::Unavailable`]) exist. A [`FrameWriter`] opened for it writes it.
pub const PROTOCOL_VERSION_2: u16 = 2;

/// The largest frame or version 2 part this build will read, and the bound
/// that is applied to a
/// length **before** anything is allocated for it.
///
/// A corrupt or hostile four-byte length is otherwise an allocation of up to
/// 4 GiB, which is an out-of-memory rather than an error. 64 MiB is eight
/// times the default `SealPolicy::max_bytes`. A publisher splits an interval's
/// rows to stay under it. On protocol version 1 a `Segment` cannot be split,
/// so one whose frame exceeds it cannot be sent; on version 2 a larger frame
/// travels in parts of at most this size, up to [`MAX_SPLIT_FRAME_BYTES`].
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// The largest frame protocol version 2 carries in parts, and the most a
/// reader joins for one: 1 GiB. A segment's parquet file and its caller index
/// are separate SQLite values, each up to SQLite's default limit of
/// 1,000,000,000 bytes, so a segment whose two together exceed 1 GiB exists
/// and cannot be sent. A reader on a 32-bit target may fail to allocate a
/// frame near this size.
pub const MAX_SPLIT_FRAME_BYTES: usize = 1 << 30;

/// The bytes of a version 2 frame's number, between its length and its kind.
const NUMBER_BYTES: usize = 8;

/// The bytes a `Rows` frame's body takes before its first row: kind, source,
/// `seq`, `index_state` and the row count (WIRE.md §4.3).
#[cfg_attr(not(feature = "archive"), allow(dead_code))]
pub(crate) const ROWS_FRAME_HEADER_BYTES: usize = 1 + 4 + 8 + 8 + 8 + 4;

/// The bytes one row adds to a `Rows` frame's body.
#[cfg_attr(not(feature = "archive"), allow(dead_code))]
pub(crate) fn rows_frame_row_bytes(row: &WalRow) -> usize {
    4 + row.stream.len() + 8 + 8 + 4 + row.row.len()
}

/// The bytes of a `Segment` frame's body (WIRE.md §4.4).
#[cfg_attr(not(feature = "archive"), allow(dead_code))]
pub(crate) fn segment_frame_bytes(
    stream: &str,
    bytes: &[u8],
    caller_index: Option<&[u8]>,
) -> usize {
    1 + 4
        + (4 + stream.len())
        + 8
        + 8
        + 8
        + (4 + bytes.len())
        + 1
        + caller_index.map_or(0, |i| 4 + i.len())
}

/// The bytes a frame's length prefix occupies, and therefore the offset at
/// which its payload begins.
///
/// [`encode`] returns a **whole frame** — length prefix, kind byte, payload —
/// while [`decode_payload`] takes the **payload alone**, because
/// [`FrameReader`] has already consumed the prefix in order to know how much to
/// read. That asymmetry is right for the reader and awkward for a caller
/// pairing the two by hand, which is what this is for:
///
/// ```
/// # use dendro::replicate::{Frame, NO_INDEX_STATE};
/// # use dendro::replicate::wire::{encode, decode_payload, LENGTH_PREFIX_BYTES};
/// let frame = Frame::Rows {
///     source: 0,
///     seq: 0,
///     index_state: NO_INDEX_STATE,
///     rows: Vec::new(),
/// };
/// let bytes = encode(&frame)?;
/// let back = decode_payload(&bytes[LENGTH_PREFIX_BYTES..])?;
/// assert_eq!(back, frame);
/// # Ok::<(), dendro::Error>(())
/// ```
///
/// A bare `4` at the call site is the kind of constant that is right until
/// somebody changes the framing, so this is what the code below counts in too.
pub const LENGTH_PREFIX_BYTES: usize = 4;

const KIND_HANDSHAKE: u8 = 1;
const KIND_INDEX: u8 = 2;
const KIND_ROWS: u8 = 3;
const KIND_SEGMENT: u8 = 4;
const KIND_CLOCK_OFFSET: u8 = 5;
const KIND_SOURCE_UPDATE: u8 = 6;
const KIND_UNAVAILABLE: u8 = 9;
const KIND_SYNC: u8 = KIND_MUST_UNDERSTAND | 7;
const KIND_SEGMENT_AFTER: u8 = KIND_MUST_UNDERSTAND | 8;
/// The first part of a split frame (WIRE.md §2.1), version 2 only.
const KIND_SPLIT_FIRST: u8 = 0xfe;
/// A later part of a split frame, version 2 only.
const KIND_SPLIT_NEXT: u8 = 0xff;

/// The high bit of a frame's kind: set on a kind a reader must understand to
/// apply the stream correctly. A reader that does not know such a kind stops
/// with an error rather than skipping it (WIRE.md §7). Of the kinds this build
/// writes, `Sync`, `SegmentAfter` and the parts of a split frame set it, all
/// version 2 only.
pub const KIND_MUST_UNDERSTAND: u8 = 0x80;

const INDEX_FULL: u8 = 0;
const INDEX_DELTA: u8 = 1;

fn malformed(what: &str) -> Error {
    Error::Message(format!("malformed replication frame: {what}"))
}

// ---------------------------------------------------------------- encoding

fn put_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// `u32` length, then the bytes. The length is `u32` rather than `u64` because
/// [`MAX_FRAME_BYTES`] bounds the whole frame well below 4 GiB anyway, and a
/// `usize` above `u32::MAX` here is a bug rather than a big payload.
fn put_bytes(out: &mut Vec<u8>, v: &[u8]) -> Result<()> {
    let len = u32::try_from(v.len()).map_err(|_| {
        Error::Message(format!(
            "a replication field of {} bytes is too large to encode",
            v.len()
        ))
    })?;
    put_u32(out, len);
    out.extend_from_slice(v);
    Ok(())
}

fn put_str(out: &mut Vec<u8>, v: &str) -> Result<()> {
    put_bytes(out, v.as_bytes())
}

/// A count, then the pairs. `BTreeMap` iterates in key order, so two encodes of
/// equal maps produce equal bytes — which is what lets a test compare frames by
/// their encoding.
fn put_map(out: &mut Vec<u8>, v: &BTreeMap<String, String>) -> Result<()> {
    let len = u32::try_from(v.len())
        .map_err(|_| Error::Message("a replication map is too large to encode".to_string()))?;
    put_u32(out, len);
    for (k, val) in v {
        put_str(out, k)?;
        put_str(out, val)?;
    }
    Ok(())
}

/// An `Option<&[u8]>` as a presence byte and, when present, the bytes. Absent
/// and empty are distinct: a segment with a zero-length index is not a segment
/// with no index.
fn put_opt_bytes(out: &mut Vec<u8>, v: Option<&[u8]>) -> Result<()> {
    match v {
        None => put_u8(out, 0),
        Some(b) => {
            put_u8(out, 1);
            put_bytes(out, b)?;
        }
    }
    Ok(())
}

/// The name of a frame that exists only in protocol version 2, or `None`.
fn version_2_only(frame: &Frame) -> Option<&'static str> {
    match frame {
        Frame::Sync { .. } => Some("Sync"),
        Frame::SegmentAfter { .. } => Some("SegmentAfter"),
        Frame::Unavailable { .. } => Some("Unavailable"),
        _ => None,
    }
}

/// Append one protocol version 1 frame's bytes — length, kind, payload — to
/// `out`.
///
/// Appends rather than returning, so a caller batching frames into one buffer
/// does not allocate per frame. A frame that exists only in version 2 is
/// refused; a [`FrameWriter`] opened for version 2 writes those. On an error
/// `out` is as it was.
pub fn encode_frame(frame: &Frame, out: &mut Vec<u8>) -> Result<()> {
    if let Some(name) = version_2_only(frame) {
        return Err(Error::Message(format!(
            "a `{name}` frame exists only in replication protocol version 2; write it \
             with a FrameWriter opened for that version"
        )));
    }
    // The length is not known until the payload is encoded, so reserve its
    // four bytes and fill them in afterwards.
    let len_at = out.len();
    put_u32(out, 0);
    let body_at = out.len();
    if let Err(e) = put_payload(frame, out) {
        out.truncate(len_at);
        return Err(e);
    }

    debug_assert_eq!(
        body_at - len_at,
        LENGTH_PREFIX_BYTES,
        "the reserved prefix and the exported offset must be the same thing"
    );
    let body = out.len() - body_at;
    if body > MAX_FRAME_BYTES {
        out.truncate(len_at);
        return Err(Error::Message(format!(
            "a replication frame of {body} bytes exceeds the {MAX_FRAME_BYTES}-byte limit"
        )));
    }
    let len = u32::try_from(body).expect("bounded by MAX_FRAME_BYTES above");
    out[len_at..body_at].copy_from_slice(&len.to_le_bytes());
    Ok(())
}

/// Append one frame's kind byte and fields to `out`: the payload, which is
/// the same in both protocol versions.
fn put_payload(frame: &Frame, out: &mut Vec<u8>) -> Result<()> {
    match frame {
        Frame::Handshake {
            source,
            uuid,
            labels,
            metadata,
            clock_anchor_wall_ns,
            complete,
        } => {
            put_u8(out, KIND_HANDSHAKE);
            put_u32(out, *source);
            put_opt_bytes(out, uuid.as_ref().map(|s| s.as_bytes()))?;
            put_map(out, labels)?;
            put_map(out, metadata)?;
            put_i64(out, *clock_anchor_wall_ns);
            put_u8(out, u8::from(*complete));
        }
        Frame::Index {
            source,
            stream,
            ts,
            kind,
            state,
            blob,
        } => {
            put_u8(out, KIND_INDEX);
            put_u32(out, *source);
            put_str(out, stream)?;
            put_i64(out, *ts);
            put_u8(
                out,
                match kind {
                    IndexKind::Full => INDEX_FULL,
                    IndexKind::Delta => INDEX_DELTA,
                },
            );
            put_u64(out, state.0);
            put_u64(out, state.1);
            put_bytes(out, blob)?;
        }
        Frame::Rows {
            source,
            seq,
            index_state,
            rows,
        } => {
            put_u8(out, KIND_ROWS);
            put_u32(out, *source);
            put_u64(out, *seq);
            put_u64(out, index_state.0);
            put_u64(out, index_state.1);
            let len = u32::try_from(rows.len()).map_err(|_| {
                Error::Message("a replication frame holds too many rows to encode".to_string())
            })?;
            put_u32(out, len);
            for row in rows {
                put_str(out, &row.stream)?;
                put_i64(out, row.ts);
                put_i64(out, row.wall_offset);
                put_bytes(out, &row.row)?;
            }
        }
        Frame::Segment {
            source,
            stream,
            meta,
            bytes,
            caller_index,
        } => {
            put_u8(out, KIND_SEGMENT);
            put_u32(out, *source);
            put_str(out, stream)?;
            put_u64(out, meta.rows);
            put_i64(out, meta.first_ts);
            put_i64(out, meta.last_ts);
            put_bytes(out, bytes)?;
            put_opt_bytes(out, caller_index.as_deref())?;
        }
        Frame::ClockOffset {
            source,
            ts,
            offset_ns,
        } => {
            put_u8(out, KIND_CLOCK_OFFSET);
            put_u32(out, *source);
            put_i64(out, *ts);
            put_i64(out, *offset_ns);
        }
        Frame::SourceUpdate {
            source,
            ts,
            labels,
            metadata,
        } => {
            put_u8(out, KIND_SOURCE_UPDATE);
            put_u32(out, *source);
            put_i64(out, *ts);
            put_map(out, labels)?;
            put_map(out, metadata)?;
        }
        Frame::Sync { source, stream, at } => {
            put_u8(out, KIND_SYNC);
            put_u32(out, *source);
            put_str(out, stream)?;
            put_i64(out, *at);
        }
        Frame::SegmentAfter {
            source,
            stream,
            after,
            meta,
            bytes,
            caller_index,
        } => {
            put_u8(out, KIND_SEGMENT_AFTER);
            put_u32(out, *source);
            put_str(out, stream)?;
            put_i64(out, *after);
            put_u64(out, meta.rows);
            put_i64(out, meta.first_ts);
            put_i64(out, meta.last_ts);
            put_bytes(out, bytes)?;
            put_opt_bytes(out, caller_index.as_deref())?;
        }
        Frame::Unavailable {
            source,
            stream,
            after,
            before,
        } => {
            put_u8(out, KIND_UNAVAILABLE);
            put_u32(out, *source);
            put_str(out, stream)?;
            put_i64(out, *after);
            put_i64(out, *before);
        }
    }
    Ok(())
}

/// One protocol version 1 frame's bytes on their own.
pub fn encode(frame: &Frame) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    encode_frame(frame, &mut out)?;
    Ok(out)
}

// ---------------------------------------------------------------- decoding

/// A cursor over one frame's payload. Every read is bounds-checked and returns
/// an error rather than panicking, because the bytes came off a wire.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Cursor { bytes, at: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .ok_or_else(|| malformed("a length overflowed"))?;
        if end > self.bytes.len() {
            return Err(malformed(&format!(
                "it ends early: {n} more byte(s) wanted, {} left",
                self.bytes.len() - self.at
            )));
        }
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self) -> Result<String> {
        let raw = self.bytes()?;
        String::from_utf8(raw).map_err(|_| malformed("a string field is not UTF-8"))
    }

    fn map(&mut self) -> Result<BTreeMap<String, String>> {
        let len = self.u32()? as usize;
        // Not `with_capacity`: `len` is off the wire, and a count far larger
        // than the bytes behind it would reserve for entries that cannot
        // arrive. Each iteration's own read is what bounds this.
        let mut out = BTreeMap::new();
        for _ in 0..len {
            let k = self.string()?;
            let v = self.string()?;
            out.insert(k, v);
        }
        Ok(out)
    }

    fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.bytes()?)),
            other => Err(malformed(&format!(
                "{other} is not a presence byte; it is 0 or 1"
            ))),
        }
    }

    fn bool(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(malformed(&format!(
                "{other} is not a boolean; it is 0 or 1"
            ))),
        }
    }

    /// Every byte of the payload must be consumed. A frame whose length says
    /// more than its fields read is one this build does not understand, and
    /// accepting it would apply a frame whose meaning it has guessed at.
    fn finish(self) -> Result<()> {
        if self.at != self.bytes.len() {
            return Err(malformed(&format!(
                "{} trailing byte(s) after its fields",
                self.bytes.len() - self.at
            )));
        }
        Ok(())
    }
}

/// Decode one protocol version 1 frame from a payload: the bytes after the
/// length prefix, kind byte included.
pub fn decode_payload(payload: &[u8]) -> Result<Frame> {
    match decode_known(payload, PROTOCOL_VERSION)? {
        Some(frame) => Ok(frame),
        None => Err(malformed(&format!(
            "{} is not a frame kind this build reads",
            payload[0]
        ))),
    }
}

/// [`decode_payload`], with a kind this build does not know returned as
/// `None` rather than an error, so [`FrameReader`] can skip it (WIRE.md §7),
/// unless the kind has [`KIND_MUST_UNDERSTAND`] set, which is an error.
/// Only the kind byte of an unknown frame is read: its layout is not known,
/// and the length prefix already says where it ends. In a version 1 stream,
/// `Sync` and `SegmentAfter` are refused and `Unavailable` is skipped as an
/// unknown kind, as a reader on 0.7.1 skips it.
fn decode_known(payload: &[u8], version: u16) -> Result<Option<Frame>> {
    let mut c = Cursor::new(payload);
    let kind = c.u8()?;
    if version < PROTOCOL_VERSION_2 {
        if matches!(kind, KIND_SYNC | KIND_SEGMENT_AFTER) {
            return Err(malformed(&format!(
                "kind {kind} exists only in protocol version 2, and this stream is version \
                 {version}"
            )));
        }
        if kind == KIND_UNAVAILABLE {
            return Ok(None);
        }
    }
    let frame = match kind {
        KIND_HANDSHAKE => {
            let source = c.u32()?;
            let uuid = match c.opt_bytes()? {
                None => None,
                Some(raw) => {
                    Some(String::from_utf8(raw).map_err(|_| malformed("a uuid is not UTF-8"))?)
                }
            };
            Frame::Handshake {
                source,
                uuid,
                labels: c.map()?,
                metadata: c.map()?,
                clock_anchor_wall_ns: c.i64()?,
                complete: c.bool()?,
            }
        }
        KIND_INDEX => Frame::Index {
            source: c.u32()?,
            stream: c.string()?,
            ts: c.i64()?,
            kind: match c.u8()? {
                INDEX_FULL => IndexKind::Full,
                INDEX_DELTA => IndexKind::Delta,
                other => return Err(malformed(&format!("{other} is not an index kind"))),
            },
            state: (c.u64()?, c.u64()?),
            blob: c.bytes()?,
        },
        KIND_ROWS => {
            let source = c.u32()?;
            let seq = c.u64()?;
            let index_state = (c.u64()?, c.u64()?);
            let count = c.u32()? as usize;
            let mut rows = Vec::new();
            for _ in 0..count {
                rows.push(WalRow {
                    stream: c.string()?,
                    ts: c.i64()?,
                    wall_offset: c.i64()?,
                    row: c.bytes()?,
                });
            }
            Frame::Rows {
                source,
                seq,
                index_state,
                rows,
            }
        }
        KIND_SEGMENT => Frame::Segment {
            source: c.u32()?,
            stream: c.string()?,
            meta: SegmentMeta {
                rows: c.u64()?,
                first_ts: c.i64()?,
                last_ts: c.i64()?,
            },
            bytes: c.bytes()?,
            caller_index: c.opt_bytes()?,
        },
        KIND_CLOCK_OFFSET => Frame::ClockOffset {
            source: c.u32()?,
            ts: c.i64()?,
            offset_ns: c.i64()?,
        },
        KIND_SOURCE_UPDATE => Frame::SourceUpdate {
            source: c.u32()?,
            ts: c.i64()?,
            labels: c.map()?,
            metadata: c.map()?,
        },
        KIND_SYNC => Frame::Sync {
            source: c.u32()?,
            stream: c.string()?,
            at: c.i64()?,
        },
        KIND_SEGMENT_AFTER => Frame::SegmentAfter {
            source: c.u32()?,
            stream: c.string()?,
            after: c.i64()?,
            meta: SegmentMeta {
                rows: c.u64()?,
                first_ts: c.i64()?,
                last_ts: c.i64()?,
            },
            bytes: c.bytes()?,
            caller_index: c.opt_bytes()?,
        },
        KIND_UNAVAILABLE => Frame::Unavailable {
            source: c.u32()?,
            stream: c.string()?,
            after: c.i64()?,
            before: c.i64()?,
        },
        KIND_SPLIT_FIRST | KIND_SPLIT_NEXT => {
            return Err(malformed(&format!(
                "kind {kind} is a part of a split frame, which exists only in protocol \
                 version 2 and only as a whole frame's part, not inside one"
            )))
        }
        unknown if unknown & KIND_MUST_UNDERSTAND != 0 => {
            return Err(malformed(&format!(
                "{unknown} is a frame kind this build does not read, and its high bit says a \
                 reader must understand it to apply the stream; a newer dendro wrote it"
            )))
        }
        _ => return Ok(None),
    };
    c.finish()?;
    Ok(Some(frame))
}

/// Reads frames off a byte stream.
///
/// Owns the stream because it must read the preamble before any frame, and a
/// caller that re-read it would get a frame whose first four bytes are the
/// magic.
pub struct FrameReader<R> {
    inner: R,
    buf: Vec<u8>,
    skipped: u64,
    version: u16,
    /// The number the next version 2 frame must carry.
    next_number: u64,
}

/// Hand-written so the reader is printable over a transport that is not, which
/// most are — `TcpStream` is, a boxed `dyn Read` is not.
impl<R> std::fmt::Debug for FrameReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameReader")
            .field("version", &self.version)
            .field("buffered", &self.buf.len())
            .field("skipped", &self.skipped)
            .finish_non_exhaustive()
    }
}

impl<R: Read> FrameReader<R> {
    /// Read and check the preamble, then be ready for frames.
    ///
    /// A stream whose magic is wrong is refused here rather than at the first
    /// frame, so pointing this at the wrong socket says so instead of
    /// reporting a malformed frame. So is a version other than
    /// [`PROTOCOL_VERSION`] and [`PROTOCOL_VERSION_2`].
    pub fn new(mut inner: R) -> Result<Self> {
        let mut magic = [0u8; 12];
        inner
            .read_exact(&mut magic)
            .map_err(|e| Error::Message(format!("failed to read the replication preamble: {e}")))?;
        if &magic != MAGIC {
            return Err(Error::Message(
                "this is not a dendro replication stream: the magic does not match".to_string(),
            ));
        }
        let mut version = [0u8; 2];
        inner
            .read_exact(&mut version)
            .map_err(|e| Error::Message(format!("failed to read the replication preamble: {e}")))?;
        let version = check_version(u16::from_le_bytes(version))?;
        Ok(FrameReader {
            inner,
            buf: Vec::new(),
            skipped: 0,
            version,
            next_number: 0,
        })
    }

    /// The protocol version the preamble named.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// How many frames of a kind this build does not know have been skipped.
    /// A publisher newer than this subscriber sends them; the subscriber
    /// holds everything else and lacks only what those frames carried.
    pub fn skipped(&self) -> u64 {
        self.skipped
    }

    /// The next frame, or `None` at a clean end of stream.
    ///
    /// A stream that ends **inside** a frame is an error, not a `None`: a
    /// truncated frame is a lost frame, and reporting it as the end would
    /// silently shorten the recording. On version 2 a frame whose number is
    /// not the next one is an error too, for the same reason, and so is a
    /// stream that ends between the parts of a split frame.
    ///
    /// A frame of a kind this build does not know is skipped and counted in
    /// [`skipped`](Self::skipped), not returned and not an error (WIRE.md
    /// §7): the length prefix says where it ends, and a subscriber older than
    /// its publisher should lose only what the new kind carries. A kind with
    /// [`KIND_MUST_UNDERSTAND`] set is the exception: it is an error, because
    /// applying the frames after it without it would be wrong. After any
    /// error the reader may be inside a frame, so a caller must stop reading
    /// the stream rather than call this again.
    pub fn next_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            let Some(at) = self.read_part()? else {
                return Ok(None);
            };
            let decoded = if self.version >= PROTOCOL_VERSION_2
                && self.buf.get(at) == Some(&KIND_SPLIT_FIRST)
            {
                let joined = self.join_split(at)?;
                decode_known(&joined, self.version)?
            } else if self.version >= PROTOCOL_VERSION_2
                && self.buf.get(at) == Some(&KIND_SPLIT_NEXT)
            {
                return Err(malformed(
                    "a later part of a split frame arrived with no first part before it",
                ));
            } else {
                decode_known(&self.buf[at..], self.version)?
            };
            match decoded {
                Some(frame) => return Ok(Some(frame)),
                None => self.skipped += 1,
            }
        }
    }

    /// The payload of a split frame whose first part is in `buf` at `at`,
    /// read from the parts after it. The bytes are joined as they arrive,
    /// never reserved from the declared total, so a total off the wire is
    /// not an allocation.
    fn join_split(&mut self, at: usize) -> Result<Vec<u8>> {
        let mut c = Cursor::new(&self.buf[at + 1..]);
        let total = c.u64()?;
        if total > MAX_SPLIT_FRAME_BYTES as u64 {
            return Err(malformed(&format!(
                "a split frame declares {total} bytes; the most a reader joins is \
                 {MAX_SPLIT_FRAME_BYTES}"
            )));
        }
        let total = total as usize;
        let mut joined = self.buf[at + 1 + 8..].to_vec();
        // Grown to `total` at most, never by doubling past it: doubling would
        // reserve up to twice the frame, which a 32-bit reader cannot address
        // for a frame near the limit.
        while joined.len() < total {
            let Some(at) = self.read_part()? else {
                return Err(Error::Message(format!(
                    "a replication stream ended inside a split frame, {} of its {total} \
                     bytes read",
                    joined.len()
                )));
            };
            if self.buf.get(at) != Some(&KIND_SPLIT_NEXT) {
                return Err(malformed(
                    "a split frame's parts are not consecutive: another frame arrived \
                     before its last part",
                ));
            }
            let part = &self.buf[at + 1..];
            let need = joined.len() + part.len();
            if need > joined.capacity() {
                let want = need
                    .max(joined.capacity().saturating_mul(2))
                    .min(total.max(need));
                joined.reserve_exact(want - joined.len());
            }
            joined.extend_from_slice(part);
        }
        if joined.len() != total {
            return Err(malformed(&format!(
                "a split frame's parts carry {} bytes, and its first part declared {total}",
                joined.len()
            )));
        }
        Ok(joined)
    }

    /// Read one length-prefixed frame or part into `buf`, and return where its
    /// kind byte is, or `None` at a clean end. On version 2 the frame's number
    /// is checked and stepped past.
    fn read_part(&mut self) -> Result<Option<usize>> {
        let Some(()) = self.read_payload()? else {
            return Ok(None);
        };
        if self.version < PROTOCOL_VERSION_2 {
            return Ok(Some(0));
        }
        if self.buf.len() < NUMBER_BYTES + 1 {
            return Err(malformed(&format!(
                "a {}-byte frame is shorter than a version 2 frame's number and kind",
                self.buf.len()
            )));
        }
        let number = u64::from_le_bytes(self.buf[..NUMBER_BYTES].try_into().expect("8 bytes"));
        if number != self.next_number {
            return Err(Error::Message(format!(
                "replication frame number {number} arrived where {} was next: a frame was \
                 lost, repeated or reordered in transit",
                self.next_number
            )));
        }
        self.next_number += 1;
        Ok(Some(NUMBER_BYTES))
    }

    /// Read one length-prefixed payload into `buf`, or `None` at a clean end.
    fn read_payload(&mut self) -> Result<Option<()>> {
        // Counted rather than `read_exact`, whose `UnexpectedEof` cannot say
        // how many bytes it read: zero bytes is the end of the stream, and one
        // to three is a frame cut inside its length prefix.
        let mut len = [0u8; LENGTH_PREFIX_BYTES];
        let mut got = 0;
        while got < LENGTH_PREFIX_BYTES {
            match self.inner.read(&mut len[got..]) {
                Ok(0) if got == 0 => return Ok(None),
                Ok(0) => {
                    return Err(Error::Message(format!(
                        "a replication stream ended {got} byte(s) into a frame's \
                         {LENGTH_PREFIX_BYTES}-byte length"
                    )))
                }
                Ok(n) => got += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => {
                    return Err(Error::Message(format!(
                        "failed to read a replication frame length: {e}"
                    )))
                }
            }
        }
        let len = u32::from_le_bytes(len) as usize;
        // Checked before the buffer is grown, which is the reason the limit
        // exists: a four-byte length off a wire is otherwise an allocation
        // request of up to 4 GiB.
        if len > MAX_FRAME_BYTES {
            return Err(Error::Message(format!(
                "a replication frame claims {len} bytes, above the {MAX_FRAME_BYTES}-byte limit"
            )));
        }
        self.buf.clear();
        self.buf.resize(len, 0);
        self.inner.read_exact(&mut self.buf).map_err(|e| {
            Error::Message(format!(
                "failed to read a {len}-byte replication frame: {e}"
            ))
        })?;
        Ok(Some(()))
    }
}

/// `version` if this build reads and writes it, else the error naming it.
fn check_version(version: u16) -> Result<u16> {
    match version {
        PROTOCOL_VERSION | PROTOCOL_VERSION_2 => Ok(version),
        _ => Err(Error::Message(format!(
            "replication protocol version {version}: this build speaks v{PROTOCOL_VERSION} \
             and v{PROTOCOL_VERSION_2}"
        ))),
    }
}

/// Write the protocol version 1 preamble. Call once, before the first frame.
pub fn write_preamble<W: Write>(out: W) -> Result<()> {
    write_preamble_for(out, PROTOCOL_VERSION)
}

fn write_preamble_for<W: Write>(mut out: W, version: u16) -> Result<()> {
    out.write_all(MAGIC)
        .and_then(|()| out.write_all(&version.to_le_bytes()))
        .map_err(|e| Error::Message(format!("failed to write the replication preamble: {e}")))
}

/// Writes a replication stream: the preamble, then frames.
///
/// For [`PROTOCOL_VERSION`] it writes the bytes [`write_preamble`] and
/// [`encode_frame`] produce. For [`PROTOCOL_VERSION_2`] it numbers every
/// frame from zero and sends a frame larger than [`MAX_FRAME_BYTES`] in parts
/// (WIRE.md §2.1), which a [`FrameReader`] joins.
///
/// After an error from [`write`](Self::write) the stream may end inside a
/// frame, so the caller must stop writing to it. A frame refused before any
/// byte was written, one too large or one the version does not have, leaves
/// the stream as it was.
pub struct FrameWriter<W> {
    out: W,
    version: u16,
    next_number: u64,
    buf: Vec<u8>,
}

impl<W> std::fmt::Debug for FrameWriter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameWriter")
            .field("version", &self.version)
            .field("next_number", &self.next_number)
            .finish_non_exhaustive()
    }
}

impl<W: Write> FrameWriter<W> {
    /// Write the preamble for `version`, then be ready for frames.
    pub fn new(mut out: W, version: u16) -> Result<Self> {
        let version = check_version(version)?;
        write_preamble_for(&mut out, version)?;
        Ok(FrameWriter {
            out,
            version,
            next_number: 0,
            buf: Vec::new(),
        })
    }

    /// The protocol version the preamble named.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Write one frame.
    pub fn write(&mut self, frame: &Frame) -> Result<()> {
        let written = self.write_in_parts_of(frame, MAX_FRAME_BYTES);
        // A split frame's buffer is not kept for the frames after it.
        if self.buf.capacity() > MAX_FRAME_BYTES {
            self.buf = Vec::new();
        }
        written
    }

    /// [`write`](Self::write), splitting at `limit` bytes per frame or part
    /// rather than at [`MAX_FRAME_BYTES`], so a test can split a small frame.
    fn write_in_parts_of(&mut self, frame: &Frame, limit: usize) -> Result<()> {
        self.buf.clear();
        if self.version < PROTOCOL_VERSION_2 {
            encode_frame(frame, &mut self.buf)?;
            return self
                .out
                .write_all(&self.buf)
                .map_err(|e| Error::Message(format!("failed to write a replication frame: {e}")));
        }
        put_payload(frame, &mut self.buf)?;
        let total = self.buf.len();
        if total > MAX_SPLIT_FRAME_BYTES {
            return Err(Error::Message(format!(
                "a replication frame of {total} bytes exceeds the \
                 {MAX_SPLIT_FRAME_BYTES}-byte limit on a split frame"
            )));
        }
        if NUMBER_BYTES + total <= limit {
            return write_part(&mut self.out, &mut self.next_number, &[], &self.buf);
        }
        // The first part carries the split kind and the total; every later
        // part carries the next kind and as many bytes as fit.
        let mut header = vec![KIND_SPLIT_FIRST];
        header.extend_from_slice(&(total as u64).to_le_bytes());
        let mut rest = &self.buf[..];
        let room = limit - NUMBER_BYTES - header.len();
        let (first, after) = rest.split_at(room.min(rest.len()));
        write_part(&mut self.out, &mut self.next_number, &header, first)?;
        rest = after;
        let room = limit - NUMBER_BYTES - 1;
        while !rest.is_empty() {
            let (part, after) = rest.split_at(room.min(rest.len()));
            write_part(
                &mut self.out,
                &mut self.next_number,
                &[KIND_SPLIT_NEXT],
                part,
            )?;
            rest = after;
        }
        Ok(())
    }

    /// The transport.
    pub fn get_ref(&self) -> &W {
        &self.out
    }

    /// The transport, to flush it.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.out
    }

    /// The transport, ending the writer.
    pub fn into_inner(self) -> W {
        self.out
    }
}

/// One version 2 frame or part: length, `number`, `head`, `body`.
fn write_part<W: Write>(out: &mut W, number: &mut u64, head: &[u8], body: &[u8]) -> Result<()> {
    let len = NUMBER_BYTES + head.len() + body.len();
    let len = u32::try_from(len).expect("a part is bounded by MAX_FRAME_BYTES");
    let mut prefix = [0u8; LENGTH_PREFIX_BYTES + NUMBER_BYTES];
    prefix[..LENGTH_PREFIX_BYTES].copy_from_slice(&len.to_le_bytes());
    prefix[LENGTH_PREFIX_BYTES..].copy_from_slice(&number.to_le_bytes());
    out.write_all(&prefix)
        .and_then(|()| out.write_all(head))
        .and_then(|()| out.write_all(body))
        .map_err(|e| Error::Message(format!("failed to write a replication frame: {e}")))?;
    *number += 1;
    Ok(())
}

// ---------------------------------------------------------- resume request

/// Written at the head of an encoded [`ResumeRequest`].
pub const RESUME_MAGIC: &[u8; 14] = b"dendro-resume\0";

impl ResumeRequest {
    /// The request's bytes (WIRE.md §8): [`RESUME_MAGIC`], the protocol
    /// version it asks for, then its fields.
    ///
    /// Refused when the request is out of order (see the type), or when it
    /// encodes to more than [`MAX_FRAME_BYTES`], so a reader can bound what
    /// it reads.
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.check()?;
        let out = self.encode_fields()?;
        if out.len() > MAX_FRAME_BYTES {
            return Err(Error::Message(format!(
                "a resume request of {} bytes exceeds the {MAX_FRAME_BYTES}-byte limit",
                out.len()
            )));
        }
        Ok(out)
    }

    /// The bytes, unchecked.
    fn encode_fields(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(RESUME_MAGIC);
        out.extend_from_slice(&PROTOCOL_VERSION_2.to_le_bytes());
        put_i64(&mut out, self.since_for_new_sources);
        put_count(&mut out, self.sources.len())?;
        for source in &self.sources {
            put_str(&mut out, &source.uuid)?;
            put_count(&mut out, source.streams.len())?;
            for stream in &source.streams {
                put_str(&mut out, &stream.stream)?;
                put_i64(&mut out, stream.newest);
                put_count(&mut out, stream.spans.len())?;
                for &(first, last) in &stream.spans {
                    put_i64(&mut out, first);
                    put_i64(&mut out, last);
                }
            }
            put_count(&mut out, source.caller_rows.len())?;
            for name in &source.caller_rows {
                put_str(&mut out, &name.name)?;
                put_i64(&mut out, name.ts);
                put_u64(&mut out, name.at_ts);
            }
        }
        Ok(out)
    }

    /// A request from its bytes, as [`encode`](Self::encode) writes them.
    ///
    /// Refused when `bytes` exceed [`MAX_FRAME_BYTES`], when the magic or
    /// the version is wrong, when bytes are left over, and when the request
    /// is out of order.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Self::decode_fields(bytes).map_err(|e| match e {
            Error::Message(m) if m.starts_with("malformed replication frame: ") => {
                Error::Message(m.replacen("replication frame", "resume request", 1))
            }
            other => other,
        })
    }

    fn decode_fields(bytes: &[u8]) -> Result<Self> {
        let bad = |what: &str| Error::Message(format!("malformed resume request: {what}"));
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(bad(&format!(
                "{} bytes, above the {MAX_FRAME_BYTES}-byte limit",
                bytes.len()
            )));
        }
        let mut c = Cursor::new(bytes);
        if c.take(RESUME_MAGIC.len()).ok() != Some(&RESUME_MAGIC[..]) {
            return Err(bad("the magic does not match"));
        }
        let version = u16::from_le_bytes(c.take(2)?.try_into().expect("2 bytes"));
        if version != PROTOCOL_VERSION_2 {
            return Err(bad(&format!(
                "it asks for protocol version {version}; this build resumes on \
                 v{PROTOCOL_VERSION_2}"
            )));
        }
        let since_for_new_sources = c.i64()?;
        let mut sources = Vec::new();
        for _ in 0..c.u32()? {
            let uuid = c.string()?;
            let mut streams = Vec::new();
            for _ in 0..c.u32()? {
                let stream = c.string()?;
                let newest = c.i64()?;
                let mut spans = Vec::new();
                for _ in 0..c.u32()? {
                    spans.push((c.i64()?, c.i64()?));
                }
                streams.push(StreamPosition {
                    stream,
                    newest,
                    spans,
                });
            }
            let mut caller_rows = Vec::new();
            for _ in 0..c.u32()? {
                caller_rows.push(CallerRowPosition {
                    name: c.string()?,
                    ts: c.i64()?,
                    at_ts: c.u64()?,
                });
            }
            sources.push(SourcePosition {
                uuid,
                streams,
                caller_rows,
            });
        }
        c.finish()?;
        let request = ResumeRequest {
            sources,
            since_for_new_sources,
        };
        request.check()?;
        Ok(request)
    }

    /// The ordering rules on the type, and each stream's spans and newest
    /// row consistent with one another.
    fn check(&self) -> Result<()> {
        let bad = |what: String| Err(Error::Message(format!("malformed resume request: {what}")));
        for pair in self.sources.windows(2) {
            if pair[0].uuid >= pair[1].uuid {
                return bad(format!(
                    "source {:?} follows {:?}; sources are in uuid order, each once",
                    pair[1].uuid, pair[0].uuid
                ));
            }
        }
        for source in &self.sources {
            for pair in source.streams.windows(2) {
                if pair[0].stream >= pair[1].stream {
                    return bad(format!(
                        "source {:?}: stream {:?} follows {:?}; streams are in name order, \
                         each once",
                        source.uuid, pair[1].stream, pair[0].stream
                    ));
                }
            }
            for stream in &source.streams {
                let what = |rule: &str| {
                    bad(format!(
                        "source {:?}, stream {:?}: {rule}",
                        source.uuid, stream.stream
                    ))
                };
                if stream.spans.iter().any(|&(first, last)| first > last) {
                    return what("a span's first_ts is above its last_ts");
                }
                if stream.spans.windows(2).any(|p| p[1].1 >= p[0].0) {
                    return what("spans are newest first and do not overlap");
                }
                if stream.watermark().is_some_and(|w| w > stream.newest) {
                    return what("the newest row is below the newest span's last_ts");
                }
            }
            for pair in source.caller_rows.windows(2) {
                if pair[0].name >= pair[1].name {
                    return bad(format!(
                        "source {:?}: caller-row name {:?} follows {:?}; names are in \
                         order, each once",
                        source.uuid, pair[1].name, pair[0].name
                    ));
                }
            }
            if let Some(name) = source.caller_rows.iter().find(|n| n.at_ts == 0) {
                return bad(format!(
                    "source {:?}, caller-row name {:?}: a position counts at least one entry",
                    source.uuid, name.name
                ));
            }
        }
        Ok(())
    }
}

/// A count as `u32`.
fn put_count(out: &mut Vec<u8>, n: usize) -> Result<()> {
    let n = u32::try_from(n)
        .map_err(|_| Error::Message("a resume request list is too long to encode".to_string()))?;
    put_u32(out, n);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replicate::frame::NO_INDEX_STATE;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Every variant, through bytes and back. The one test that has to pass
    /// before anything else here means anything.
    #[test]
    fn every_frame_round_trips() {
        let frames = vec![
            Frame::Handshake {
                source: 0,
                uuid: Some("3f2b1c4d-0000-4000-8000-000000000001".to_string()),
                labels: map(&[("host", "web-01"), ("arm", "a")]),
                metadata: map(&[("encoder", "v3")]),
                clock_anchor_wall_ns: -1_000,
                complete: true,
            },
            Frame::Index {
                source: 1,
                stream: "cpu".to_string(),
                ts: 42,
                kind: IndexKind::Full,
                state: (7, 9),
                blob: vec![1, 2, 3, 4],
            },
            Frame::Index {
                source: 1,
                stream: "cpu".to_string(),
                ts: 43,
                kind: IndexKind::Delta,
                state: (11, 13),
                blob: Vec::new(),
            },
            Frame::Rows {
                source: 2,
                seq: 99,
                index_state: (7, 9),
                rows: vec![
                    WalRow {
                        stream: "cpu".to_string(),
                        ts: 1,
                        wall_offset: -5,
                        row: vec![0xde, 0xad],
                    },
                    WalRow {
                        stream: "mem".to_string(),
                        ts: 2,
                        wall_offset: 5,
                        row: Vec::new(),
                    },
                ],
            },
            Frame::Segment {
                source: 3,
                stream: "cpu".to_string(),
                meta: SegmentMeta {
                    rows: 10,
                    first_ts: -9,
                    last_ts: 9,
                },
                bytes: b"PAR1payloadPAR1".to_vec(),
                caller_index: Some(vec![9, 9]),
            },
            Frame::ClockOffset {
                source: 4,
                ts: i64::MIN,
                offset_ns: i64::MAX,
            },
            Frame::SourceUpdate {
                source: 5,
                ts: 1_700_000_000_000_000_000,
                labels: map(&[("host", "a"), ("source", "x")]),
                metadata: map(&[("metriken.layer.user", "{}")]),
            },
            Frame::SourceUpdate {
                source: 6,
                ts: -1,
                labels: BTreeMap::new(),
                metadata: BTreeMap::new(),
            },
        ];

        for frame in &frames {
            let bytes = encode(frame).unwrap();
            let len = u32::from_le_bytes(bytes[..LENGTH_PREFIX_BYTES].try_into().unwrap()) as usize;
            assert_eq!(
                len,
                bytes.len() - LENGTH_PREFIX_BYTES,
                "the length prefix covers the payload"
            );
            let back = decode_payload(&bytes[LENGTH_PREFIX_BYTES..]).unwrap();
            assert_eq!(&back, frame);
        }
    }

    /// Absent and empty are different, on both fields that carry an option.
    #[test]
    fn absent_is_not_empty() {
        for (absent, empty) in [
            (
                Frame::Segment {
                    source: 0,
                    stream: "s".to_string(),
                    meta: SegmentMeta {
                        rows: 1,
                        first_ts: 0,
                        last_ts: 0,
                    },
                    bytes: vec![1],
                    caller_index: None,
                },
                Frame::Segment {
                    source: 0,
                    stream: "s".to_string(),
                    meta: SegmentMeta {
                        rows: 1,
                        first_ts: 0,
                        last_ts: 0,
                    },
                    bytes: vec![1],
                    caller_index: Some(Vec::new()),
                },
            ),
            (
                Frame::Handshake {
                    source: 0,
                    uuid: None,
                    labels: BTreeMap::new(),
                    metadata: BTreeMap::new(),
                    clock_anchor_wall_ns: 0,
                    complete: false,
                },
                Frame::Handshake {
                    source: 0,
                    uuid: Some(String::new()),
                    labels: BTreeMap::new(),
                    metadata: BTreeMap::new(),
                    clock_anchor_wall_ns: 0,
                    complete: false,
                },
            ),
        ] {
            let a = encode(&absent).unwrap();
            let b = encode(&empty).unwrap();
            assert_ne!(a, b, "absent and empty must not encode alike");
            assert_eq!(decode_payload(&a[LENGTH_PREFIX_BYTES..]).unwrap(), absent);
            assert_eq!(decode_payload(&b[LENGTH_PREFIX_BYTES..]).unwrap(), empty);
        }
    }

    /// An empty `Rows` frame is the keepalive, so it has to survive the codec.
    #[test]
    fn the_keepalive_round_trips() {
        let frame = Frame::Rows {
            source: 0,
            seq: 0,
            index_state: NO_INDEX_STATE,
            rows: Vec::new(),
        };
        let bytes = encode(&frame).unwrap();
        assert_eq!(
            decode_payload(&bytes[LENGTH_PREFIX_BYTES..]).unwrap(),
            frame
        );
    }

    /// Bytes off a wire are not to be trusted: every truncation of every frame
    /// is an error, and none of them panics.
    #[test]
    fn a_truncated_frame_is_an_error_not_a_panic() {
        let frame = Frame::Rows {
            source: 1,
            seq: 2,
            index_state: (3, 4),
            rows: vec![WalRow {
                stream: "cpu".to_string(),
                ts: 5,
                wall_offset: 6,
                row: vec![7, 8, 9],
            }],
        };
        let bytes = encode(&frame).unwrap();
        let payload = &bytes[LENGTH_PREFIX_BYTES..];
        for cut in 0..payload.len() {
            assert!(
                decode_payload(&payload[..cut]).is_err(),
                "a payload cut to {cut} byte(s) decoded as a frame"
            );
        }
    }

    /// Trailing bytes mean the sender put something there this build does not
    /// read. Applying the fields it did understand would be acting on a frame
    /// it has guessed the meaning of.
    #[test]
    fn trailing_bytes_are_refused() {
        let frame = Frame::ClockOffset {
            source: 0,
            ts: 1,
            offset_ns: 2,
        };
        let bytes = encode(&frame).unwrap();
        let mut payload = bytes[LENGTH_PREFIX_BYTES..].to_vec();
        payload.push(0);
        assert!(decode_payload(&payload).is_err());
    }

    #[test]
    fn an_unknown_kind_is_refused() {
        assert!(decode_payload(&[200, 0, 0, 0, 0]).is_err());
    }

    /// A length above the cap is refused before anything is allocated for it.
    #[test]
    fn an_oversized_length_is_refused_before_allocating() {
        let mut stream = Vec::new();
        write_preamble(&mut stream).unwrap();
        stream.extend_from_slice(&u32::MAX.to_le_bytes());
        let mut reader = FrameReader::new(std::io::Cursor::new(stream)).unwrap();
        let err = reader.next_frame().unwrap_err().to_string();
        assert!(err.contains("limit"), "{err}");
    }

    /// Several frames through a reader, and a clean end.
    #[test]
    fn a_stream_reads_back_in_order() {
        let frames = vec![
            Frame::Handshake {
                source: 0,
                uuid: None,
                labels: map(&[("host", "a")]),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 1,
                complete: false,
            },
            Frame::Rows {
                source: 0,
                seq: 0,
                index_state: NO_INDEX_STATE,
                rows: Vec::new(),
            },
            Frame::ClockOffset {
                source: 0,
                ts: 2,
                offset_ns: 3,
            },
        ];
        let mut bytes = Vec::new();
        write_preamble(&mut bytes).unwrap();
        for f in &frames {
            encode_frame(f, &mut bytes).unwrap();
        }

        let mut reader = FrameReader::new(std::io::Cursor::new(bytes)).unwrap();
        for want in &frames {
            assert_eq!(reader.next_frame().unwrap().as_ref(), Some(want));
        }
        assert_eq!(reader.next_frame().unwrap(), None, "a clean end of stream");
    }

    /// A stream that stops inside a frame lost that frame. Reporting it as the
    /// end of the stream would silently shorten the recording.
    #[test]
    fn a_stream_cut_mid_frame_is_an_error() {
        let mut bytes = Vec::new();
        write_preamble(&mut bytes).unwrap();
        encode_frame(
            &Frame::ClockOffset {
                source: 0,
                ts: 1,
                offset_ns: 2,
            },
            &mut bytes,
        )
        .unwrap();
        bytes.truncate(bytes.len() - 3);
        let mut reader = FrameReader::new(std::io::Cursor::new(bytes)).unwrap();
        assert!(reader.next_frame().is_err());
    }

    /// A stream that stops inside a frame's length prefix also lost a frame.
    /// Only a stream that stops between frames has ended cleanly.
    #[test]
    fn a_stream_cut_inside_a_length_prefix_is_an_error() {
        let frame = Frame::ClockOffset {
            source: 0,
            ts: 1,
            offset_ns: 2,
        };
        for cut in 1..LENGTH_PREFIX_BYTES {
            let mut bytes = Vec::new();
            write_preamble(&mut bytes).unwrap();
            encode_frame(&frame, &mut bytes).unwrap();
            let next = bytes.len();
            encode_frame(&frame, &mut bytes).unwrap();
            bytes.truncate(next + cut);
            let mut reader = FrameReader::new(std::io::Cursor::new(bytes)).unwrap();
            assert_eq!(reader.next_frame().unwrap(), Some(frame.clone()));
            assert!(
                reader.next_frame().is_err(),
                "{cut} byte(s) of a length prefix read as a clean end"
            );
        }
        // Zero bytes of the next prefix is the clean end.
        let mut bytes = Vec::new();
        write_preamble(&mut bytes).unwrap();
        encode_frame(&frame, &mut bytes).unwrap();
        let mut reader = FrameReader::new(std::io::Cursor::new(bytes)).unwrap();
        assert_eq!(reader.next_frame().unwrap(), Some(frame));
        assert_eq!(reader.next_frame().unwrap(), None);
    }

    /// The size arithmetic the publisher splits by is the encoder's own.
    #[test]
    fn frame_sizes_match_the_encoding() {
        let rows: Vec<WalRow> = ["s", "stream-two"]
            .iter()
            .enumerate()
            .map(|(i, stream)| WalRow {
                stream: stream.to_string(),
                ts: i as i64,
                wall_offset: 3,
                row: vec![7; 10 * (i + 1)],
            })
            .collect();
        let expected =
            ROWS_FRAME_HEADER_BYTES + rows.iter().map(rows_frame_row_bytes).sum::<usize>();
        let frame = Frame::Rows {
            source: 1,
            seq: 2,
            index_state: (3, 4),
            rows,
        };
        assert_eq!(
            encode(&frame).unwrap().len() - LENGTH_PREFIX_BYTES,
            expected
        );

        for caller_index in [None, Some(vec![1u8; 5])] {
            let frame = Frame::Segment {
                source: 1,
                stream: "s".to_string(),
                meta: SegmentMeta {
                    rows: 1,
                    first_ts: 1,
                    last_ts: 1,
                },
                bytes: vec![9; 20],
                caller_index: caller_index.clone(),
            };
            assert_eq!(
                encode(&frame).unwrap().len() - LENGTH_PREFIX_BYTES,
                segment_frame_bytes("s", &[9; 20], caller_index.as_deref())
            );
        }
    }

    #[test]
    fn a_wrong_preamble_says_so() {
        let err = FrameReader::new(std::io::Cursor::new(b"not a stream".to_vec()))
            .unwrap_err()
            .to_string();
        assert!(err.contains("not a dendro replication stream"), "{err}");

        let mut wrong_version = MAGIC.to_vec();
        wrong_version.extend_from_slice(&99u16.to_le_bytes());
        let err = FrameReader::new(std::io::Cursor::new(wrong_version))
            .unwrap_err()
            .to_string();
        assert!(err.contains("protocol version 99"), "{err}");
        let err = FrameWriter::new(Vec::new(), 3).unwrap_err().to_string();
        assert!(err.contains("protocol version 3"), "{err}");
    }

    fn segment(len: usize) -> Frame {
        Frame::Segment {
            source: 0,
            stream: "cpu".to_string(),
            meta: SegmentMeta {
                rows: 3,
                first_ts: 1,
                last_ts: 3,
            },
            bytes: (0..len).map(|i| i as u8).collect(),
            caller_index: Some(vec![4, 5]),
        }
    }

    fn version_2_frames() -> Vec<Frame> {
        vec![
            Frame::Handshake {
                source: 0,
                uuid: Some("3f2b1c4d-0000-4000-8000-000000000001".to_string()),
                labels: map(&[("host", "a")]),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 1,
                complete: false,
            },
            Frame::Sync {
                source: 0,
                stream: "cpu".to_string(),
                at: -7,
            },
            Frame::SegmentAfter {
                source: 0,
                stream: "cpu".to_string(),
                after: -7,
                meta: SegmentMeta {
                    rows: 2,
                    first_ts: 1,
                    last_ts: 9,
                },
                bytes: b"PAR1PAR1".to_vec(),
                caller_index: None,
            },
            Frame::Unavailable {
                source: 0,
                stream: "mem".to_string(),
                after: i64::MIN,
                before: 40,
            },
            Frame::Rows {
                source: 0,
                seq: 1,
                index_state: NO_INDEX_STATE,
                rows: Vec::new(),
            },
            segment(10),
        ]
    }

    fn read_all(bytes: Vec<u8>) -> Result<Vec<Frame>> {
        let mut reader = FrameReader::new(std::io::Cursor::new(bytes))?;
        let mut out = Vec::new();
        while let Some(frame) = reader.next_frame()? {
            out.push(frame);
        }
        Ok(out)
    }

    #[test]
    fn every_frame_round_trips_through_a_version_2_stream() {
        let frames = version_2_frames();
        let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION_2).unwrap();
        for f in &frames {
            w.write(f).unwrap();
        }
        let bytes = w.into_inner();
        assert_eq!(&bytes[12..14], &2u16.to_le_bytes(), "the preamble names v2");
        let reader = FrameReader::new(std::io::Cursor::new(bytes.clone())).unwrap();
        assert_eq!(reader.version(), PROTOCOL_VERSION_2);
        assert_eq!(read_all(bytes).unwrap(), frames);
    }

    /// A version 1 `FrameWriter` writes what `write_preamble` and
    /// `encode_frame` write, byte for byte.
    #[test]
    fn a_version_1_writer_writes_the_version_1_bytes() {
        let frames = &version_2_frames()[4..];
        let mut by_hand = Vec::new();
        write_preamble(&mut by_hand).unwrap();
        for f in frames {
            encode_frame(f, &mut by_hand).unwrap();
        }
        let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION).unwrap();
        for f in frames {
            w.write(f).unwrap();
        }
        assert_eq!(w.into_inner(), by_hand);
    }

    #[test]
    fn version_2_frames_are_refused_on_version_1() {
        for frame in &version_2_frames()[1..4] {
            let err = encode(frame).unwrap_err().to_string();
            assert!(
                err.contains("only in replication protocol version 2"),
                "{err}"
            );
            let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION).unwrap();
            assert!(w.write(frame).is_err());
            assert_eq!(w.into_inner().len(), 14, "nothing after the preamble");

            // A version 1 stream that carries one anyway: `Sync` and
            // `SegmentAfter` are refused, and `Unavailable` is skipped as a
            // reader on 0.7.1 skips it.
            let mut payload = Vec::new();
            put_payload(frame, &mut payload).unwrap();
            assert!(decode_payload(&payload).is_err());
            let mut bytes = Vec::new();
            write_preamble(&mut bytes).unwrap();
            put_u32(&mut bytes, payload.len() as u32);
            bytes.extend_from_slice(&payload);
            if matches!(frame, Frame::Unavailable { .. }) {
                let mut reader = FrameReader::new(std::io::Cursor::new(bytes)).unwrap();
                assert_eq!(reader.next_frame().unwrap(), None);
                assert_eq!(reader.skipped(), 1);
            } else {
                let err = read_all(bytes).unwrap_err().to_string();
                assert!(err.contains("only in protocol version 2"), "{err}");
            }
        }
    }

    /// Version 1 bytes, written out: the layout WIRE.md §2 and §4.5 give,
    /// checked against the encoder rather than against itself.
    #[test]
    fn a_version_1_frame_has_the_documented_bytes() {
        let frame = Frame::ClockOffset {
            source: 1,
            ts: 2,
            offset_ns: -1,
        };
        let mut want = Vec::new();
        want.extend_from_slice(b"dendro-repl\0");
        want.extend_from_slice(&[1, 0]);
        want.extend_from_slice(&21u32.to_le_bytes());
        want.push(5);
        want.extend_from_slice(&1u32.to_le_bytes());
        want.extend_from_slice(&2i64.to_le_bytes());
        want.extend_from_slice(&(-1i64).to_le_bytes());
        let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION).unwrap();
        w.write(&frame).unwrap();
        assert_eq!(w.into_inner(), want);

        // And version 2: the same payload behind a number.
        let mut want2 = Vec::new();
        want2.extend_from_slice(b"dendro-repl\0");
        want2.extend_from_slice(&[2, 0]);
        want2.extend_from_slice(&29u32.to_le_bytes());
        want2.extend_from_slice(&0u64.to_le_bytes());
        want2.extend_from_slice(&want[18..]);
        let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION_2).unwrap();
        w.write(&frame).unwrap();
        assert_eq!(w.into_inner(), want2);
    }

    /// A frame whose number and payload fill the limit exactly goes as one
    /// frame; one byte more and it goes in parts, none above the limit.
    #[test]
    fn a_frame_splits_only_above_the_limit() {
        let limit = 64;
        let header = segment(0);
        let mut payload = Vec::new();
        put_payload(&header, &mut payload).unwrap();
        let base = payload.len();
        for (extra, parts) in [
            (limit - NUMBER_BYTES - base, 1),
            (limit - NUMBER_BYTES - base + 1, 2),
        ] {
            let frame = segment(extra);
            let (bytes, starts) = split(&frame, limit);
            assert_eq!(starts.len(), parts, "{extra} bytes of segment");
            for &at in &starts {
                let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
                assert!(len <= limit, "a {len}-byte part, above {limit}");
            }
            assert_eq!(read_all(bytes).unwrap(), vec![frame]);
        }
    }

    /// Frame numbers detect a frame lost, repeated or reordered in transit.
    #[test]
    fn a_lost_or_repeated_version_2_frame_is_an_error() {
        let frame = Frame::ClockOffset {
            source: 0,
            ts: 1,
            offset_ns: 2,
        };
        let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION_2).unwrap();
        let mut ends = vec![w.get_ref().len()];
        for _ in 0..3 {
            w.write(&frame).unwrap();
            ends.push(w.get_ref().len());
        }
        let bytes = w.into_inner();
        assert_eq!(read_all(bytes.clone()).unwrap().len(), 3);

        let mut lost = bytes[..ends[1]].to_vec();
        lost.extend_from_slice(&bytes[ends[2]..]);
        let err = read_all(lost).unwrap_err().to_string();
        assert!(err.contains("number 2 arrived where 1 was next"), "{err}");

        let mut repeated = bytes[..ends[2]].to_vec();
        repeated.extend_from_slice(&bytes[ends[1]..]);
        let err = read_all(repeated).unwrap_err().to_string();
        assert!(err.contains("number 1 arrived where 2 was next"), "{err}");
    }

    /// An unknown kind is skipped on version 2 as on version 1, and its
    /// number still counts.
    #[test]
    fn an_unknown_kind_is_skipped_on_version_2() {
        let frame = Frame::ClockOffset {
            source: 0,
            ts: 1,
            offset_ns: 2,
        };
        let mut bytes = Vec::new();
        write_preamble_for(&mut bytes, PROTOCOL_VERSION_2).unwrap();
        let mut number = 0;
        write_part(&mut bytes, &mut number, &[50], &[1, 2, 3]).unwrap();
        let mut payload = Vec::new();
        put_payload(&frame, &mut payload).unwrap();
        write_part(&mut bytes, &mut number, &[], &payload).unwrap();
        let mut reader = FrameReader::new(std::io::Cursor::new(bytes)).unwrap();
        assert_eq!(reader.next_frame().unwrap(), Some(frame));
        assert_eq!(reader.skipped(), 1);
        assert_eq!(reader.next_frame().unwrap(), None);
    }

    /// A frame above the part limit goes in parts and is read back whole,
    /// and the frame after it reads with the next number.
    #[test]
    fn a_split_frame_round_trips() {
        let big = segment(200);
        let after = Frame::ClockOffset {
            source: 0,
            ts: 1,
            offset_ns: 2,
        };
        let (bytes, starts) = split(&big, 40);
        assert!(starts.len() > 5, "{} parts", starts.len());
        for &at in &starts {
            let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
            assert!(len <= 40, "a {len}-byte part, above the 40-byte limit");
        }
        let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION_2).unwrap();
        w.write_in_parts_of(&big, 40).unwrap();
        w.write_in_parts_of(&after, 40).unwrap();
        assert_eq!(read_all(w.into_inner()).unwrap(), vec![big, after]);
    }

    fn split(frame: &Frame, limit: usize) -> (Vec<u8>, Vec<usize>) {
        let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION_2).unwrap();
        w.write_in_parts_of(frame, limit).unwrap();
        let bytes = w.into_inner();
        // Where each part starts.
        let mut starts = Vec::new();
        let mut at = 14;
        while at < bytes.len() {
            starts.push(at);
            let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
            at += 4 + len;
        }
        (bytes, starts)
    }

    /// The joined buffer grows to the declared total and no further, so a
    /// frame near `MAX_SPLIT_FRAME_BYTES` does not reserve twice its size.
    #[test]
    fn a_split_frame_is_joined_into_no_more_than_its_total() {
        let frame = segment(1000);
        let mut payload = Vec::new();
        put_payload(&frame, &mut payload).unwrap();
        let (bytes, _) = split(&frame, 40);
        let mut reader = FrameReader::new(std::io::Cursor::new(bytes)).unwrap();
        let at = reader.read_part().unwrap().unwrap();
        assert_eq!(reader.buf[at], KIND_SPLIT_FIRST);
        let joined = reader.join_split(at).unwrap();
        assert_eq!(joined, payload);
        assert_eq!(joined.capacity(), payload.len());
    }

    #[test]
    fn a_stream_that_ends_between_the_parts_of_a_frame_is_an_error() {
        let (bytes, starts) = split(&segment(200), 40);
        let err = read_all(bytes[..starts[3]].to_vec())
            .unwrap_err()
            .to_string();
        assert!(err.contains("ended inside a split frame"), "{err}");
    }

    #[test]
    fn a_split_frame_interrupted_by_another_frame_is_an_error() {
        let (bytes, starts) = split(&segment(200), 40);
        let mut cut = bytes[..starts[2]].to_vec();
        let mut number = 2;
        let mut payload = Vec::new();
        put_payload(
            &Frame::ClockOffset {
                source: 0,
                ts: 1,
                offset_ns: 2,
            },
            &mut payload,
        )
        .unwrap();
        write_part(&mut cut, &mut number, &[], &payload).unwrap();
        let err = read_all(cut).unwrap_err().to_string();
        assert!(err.contains("not consecutive"), "{err}");
    }

    #[test]
    fn a_later_part_with_no_first_part_is_an_error() {
        let mut bytes = Vec::new();
        write_preamble_for(&mut bytes, PROTOCOL_VERSION_2).unwrap();
        write_part(&mut bytes, &mut 0, &[KIND_SPLIT_NEXT], &[1, 2]).unwrap();
        let err = read_all(bytes).unwrap_err().to_string();
        assert!(err.contains("no first part"), "{err}");
    }

    /// The declared total bounds the join, and the parts must add up to it:
    /// parts that carry a whole frame past a smaller total are refused.
    #[test]
    fn a_split_frame_whose_parts_disagree_with_its_total_is_an_error() {
        let mut payload = Vec::new();
        put_payload(
            &Frame::ClockOffset {
                source: 0,
                ts: 1,
                offset_ns: 2,
            },
            &mut payload,
        )
        .unwrap();
        let mut bytes = Vec::new();
        write_preamble_for(&mut bytes, PROTOCOL_VERSION_2).unwrap();
        let mut head = vec![KIND_SPLIT_FIRST];
        head.extend_from_slice(&(payload.len() as u64 - 1).to_le_bytes());
        let mut number = 0;
        write_part(&mut bytes, &mut number, &head, &payload[..10]).unwrap();
        write_part(&mut bytes, &mut number, &[KIND_SPLIT_NEXT], &payload[10..]).unwrap();
        let err = read_all(bytes).unwrap_err().to_string();
        assert!(err.contains("its first part declared"), "{err}");

        for (total, parts, want) in [
            (
                (MAX_SPLIT_FRAME_BYTES + 1) as u64,
                1,
                "the most a reader joins",
            ),
            (0, 1, "its first part declared"),
            (5, 2, "its first part declared"),
        ] {
            let mut bytes = Vec::new();
            write_preamble_for(&mut bytes, PROTOCOL_VERSION_2).unwrap();
            let mut head = vec![KIND_SPLIT_FIRST];
            head.extend_from_slice(&total.to_le_bytes());
            let mut number = 0;
            write_part(&mut bytes, &mut number, &head, &[KIND_CLOCK_OFFSET, 0, 0]).unwrap();
            for _ in 1..parts {
                write_part(&mut bytes, &mut number, &[KIND_SPLIT_NEXT], &[0, 0, 0]).unwrap();
            }
            let err = read_all(bytes).unwrap_err().to_string();
            assert!(err.contains(want), "total {total}: {err}");
        }
    }

    /// A split part inside a joined frame is not a frame.
    #[test]
    fn a_split_frame_inside_a_split_frame_is_an_error() {
        let mut bytes = Vec::new();
        write_preamble_for(&mut bytes, PROTOCOL_VERSION_2).unwrap();
        let mut head = vec![KIND_SPLIT_FIRST];
        head.extend_from_slice(&3u64.to_le_bytes());
        write_part(&mut bytes, &mut 0, &head, &[KIND_SPLIT_NEXT, 0, 0]).unwrap();
        let err = read_all(bytes).unwrap_err().to_string();
        assert!(err.contains("part of a split frame"), "{err}");
    }

    /// A segment above `MAX_FRAME_BYTES` cannot travel on version 1 and does
    /// on version 2.
    #[test]
    fn a_segment_above_the_frame_limit_travels_on_version_2() {
        let big = segment(MAX_FRAME_BYTES + 1);
        assert!(encode(&big).is_err());
        let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION_2).unwrap();
        w.write(&big).unwrap();
        assert_eq!(w.next_number, 2, "two parts");
        assert_eq!(w.buf.capacity(), 0, "the split frame's buffer is not kept");
        assert_eq!(read_all(w.into_inner()).unwrap(), vec![big]);
    }

    fn request() -> ResumeRequest {
        ResumeRequest {
            sources: vec![
                SourcePosition {
                    uuid: "a".to_string(),
                    streams: vec![
                        StreamPosition {
                            stream: "cpu".to_string(),
                            newest: 50,
                            spans: vec![(30, 40), (-5, 20)],
                        },
                        StreamPosition {
                            stream: "mem".to_string(),
                            newest: i64::MIN,
                            spans: Vec::new(),
                        },
                    ],
                    caller_rows: vec![CallerRowPosition {
                        name: "idx".to_string(),
                        ts: 9,
                        at_ts: 2,
                    }],
                },
                SourcePosition {
                    uuid: "b".to_string(),
                    streams: Vec::new(),
                    caller_rows: Vec::new(),
                },
            ],
            since_for_new_sources: 7,
        }
    }

    #[test]
    fn a_resume_request_round_trips() {
        for r in [request(), ResumeRequest::default()] {
            let bytes = r.encode().unwrap();
            assert_eq!(&bytes[..14], RESUME_MAGIC);
            assert_eq!(ResumeRequest::decode(&bytes).unwrap(), r);
        }
        assert_eq!(request().sources[0].streams[0].watermark(), Some(40));
        assert_eq!(request().sources[0].streams[1].watermark(), None);
    }

    #[test]
    fn a_truncated_or_padded_resume_request_is_an_error() {
        let bytes = request().encode().unwrap();
        for cut in 0..bytes.len() {
            assert!(
                ResumeRequest::decode(&bytes[..cut]).is_err(),
                "cut at {cut}"
            );
        }
        let mut padded = bytes.clone();
        padded.push(0);
        assert!(ResumeRequest::decode(&padded).is_err());
        let mut wrong = bytes.clone();
        wrong[14] = 1;
        let err = ResumeRequest::decode(&wrong).unwrap_err().to_string();
        assert!(err.contains("protocol version 1"), "{err}");
        let mut wrong = bytes.clone();
        wrong[0] = b'x';
        let err = ResumeRequest::decode(&wrong).unwrap_err().to_string();
        assert!(err.contains("magic"), "{err}");
        let err = ResumeRequest::decode(&bytes[..20]).unwrap_err().to_string();
        assert!(
            err.starts_with("malformed resume request: it ends early"),
            "{err}"
        );
    }

    /// Each ordering rule is checked on both encode and decode.
    #[test]
    fn a_resume_request_out_of_order_is_refused() {
        type Change = Box<dyn Fn(&mut ResumeRequest)>;
        let cases: Vec<(&str, Change)> = vec![
            ("uuid order", Box::new(|r| r.sources.swap(0, 1))),
            (
                "each once",
                Box::new(|r| r.sources[1].uuid = "a".to_string()),
            ),
            ("name order", Box::new(|r| r.sources[0].streams.swap(0, 1))),
            (
                "first_ts is above",
                Box::new(|r| r.sources[0].streams[0].spans[1] = (21, 20)),
            ),
            (
                "do not overlap",
                Box::new(|r| r.sources[0].streams[0].spans[1] = (-5, 30)),
            ),
            (
                "do not overlap",
                Box::new(|r| r.sources[0].streams[0].spans.swap(0, 1)),
            ),
            (
                "newest row is below",
                Box::new(|r| r.sources[0].streams[0].newest = 39),
            ),
            (
                "names are in",
                Box::new(|r| {
                    let again = r.sources[0].caller_rows[0].clone();
                    r.sources[0].caller_rows.push(again)
                }),
            ),
            (
                "at least one entry",
                Box::new(|r| r.sources[0].caller_rows[0].at_ts = 0),
            ),
        ];
        let good = request().encode().unwrap();
        for (want, change) in cases {
            let mut r = request();
            change(&mut r);
            let err = r.encode().unwrap_err().to_string();
            assert!(err.contains(want), "{want}: {err}");
            // The same request reaching decode.
            let bytes = r.encode_fields().unwrap();
            assert_ne!(bytes, good);
            let err = ResumeRequest::decode(&bytes).unwrap_err().to_string();
            assert!(err.contains(want), "{want}: {err}");
        }
    }
}
