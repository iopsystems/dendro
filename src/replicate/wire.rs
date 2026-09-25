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

/// Written once at the head of a stream, before any frame.
pub const MAGIC: &[u8; 12] = b"dendro-repl\0";

/// The protocol version this build writes and reads. Bumped when a frame's
/// layout changes in a way an older reader would misread; a new frame kind an
/// older reader can skip does not need one, because the length prefix makes
/// skipping possible.
pub const PROTOCOL_VERSION: u16 = 1;

/// The largest frame this build will read, and the bound that is applied to a
/// length **before** anything is allocated for it.
///
/// A corrupt or hostile four-byte length is otherwise an allocation of up to
/// 4 GiB, which is an out-of-memory rather than an error. 64 MiB is eight
/// times the default `SealPolicy::max_bytes`, so the largest frame a caller on
/// the default policy can produce — a `Segment` — has ample headroom, and a
/// caller that seals larger than this is asking for a bound it chose.
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

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
const KIND_STREAM_SUMMARY: u8 = 6;

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

/// Append one frame's bytes — length, kind, payload — to `out`.
///
/// Appends rather than returning, so a caller batching frames into one buffer
/// does not allocate per frame.
pub fn encode_frame(frame: &Frame, out: &mut Vec<u8>) -> Result<()> {
    // The length is not known until the payload is encoded, so reserve its
    // four bytes and fill them in afterwards.
    let len_at = out.len();
    put_u32(out, 0);
    let body_at = out.len();

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
        Frame::StreamSummary {
            source,
            stream,
            as_of_ts,
            blob,
        } => {
            put_u8(out, KIND_STREAM_SUMMARY);
            put_u32(out, *source);
            put_str(out, stream)?;
            put_i64(out, *as_of_ts);
            put_bytes(out, blob)?;
        }
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

/// One frame's bytes on their own.
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

/// Decode one frame from a payload: the bytes after the length prefix, kind
/// byte included.
pub fn decode_payload(payload: &[u8]) -> Result<Frame> {
    match decode_known(payload)? {
        Some(frame) => Ok(frame),
        None => Err(malformed(&format!(
            "{} is not a frame kind this build reads",
            payload[0]
        ))),
    }
}

/// [`decode_payload`], with a kind this build does not know returned as
/// `None` rather than an error, so [`FrameReader`] can skip it (WIRE.md §7).
/// Only the kind byte of an unknown frame is read: its layout is not known,
/// and the length prefix already says where it ends.
fn decode_known(payload: &[u8]) -> Result<Option<Frame>> {
    let mut c = Cursor::new(payload);
    let kind = c.u8()?;
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
        KIND_STREAM_SUMMARY => Frame::StreamSummary {
            source: c.u32()?,
            stream: c.string()?,
            as_of_ts: c.i64()?,
            blob: c.bytes()?,
        },
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
}

/// Hand-written so the reader is printable over a transport that is not, which
/// most are — `TcpStream` is, a boxed `dyn Read` is not.
impl<R> std::fmt::Debug for FrameReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameReader")
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
    /// reporting a malformed frame.
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
        let version = u16::from_le_bytes(version);
        if version != PROTOCOL_VERSION {
            return Err(Error::Message(format!(
                "replication protocol version {version}: this build speaks v{PROTOCOL_VERSION}"
            )));
        }
        Ok(FrameReader {
            inner,
            buf: Vec::new(),
            skipped: 0,
        })
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
    /// silently shorten the recording.
    ///
    /// A frame of a kind this build does not know is skipped and counted in
    /// [`skipped`](Self::skipped), not returned and not an error (WIRE.md
    /// §7): the length prefix says where it ends, and a subscriber older than
    /// its publisher should lose only what the new kind carries.
    pub fn next_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            let Some(()) = self.read_payload()? else {
                return Ok(None);
            };
            match decode_known(&self.buf)? {
                Some(frame) => return Ok(Some(frame)),
                None => self.skipped += 1,
            }
        }
    }

    /// Read one length-prefixed payload into `buf`, or `None` at a clean end.
    fn read_payload(&mut self) -> Result<Option<()>> {
        let mut len = [0u8; LENGTH_PREFIX_BYTES];
        match self.inner.read_exact(&mut len) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => {
                return Err(Error::Message(format!(
                    "failed to read a replication frame length: {e}"
                )))
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

/// Write the preamble. Call once, before the first frame.
pub fn write_preamble<W: Write>(mut out: W) -> Result<()> {
    out.write_all(MAGIC)
        .and_then(|()| out.write_all(&PROTOCOL_VERSION.to_le_bytes()))
        .map_err(|e| Error::Message(format!("failed to write the replication preamble: {e}")))
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
            Frame::StreamSummary {
                source: 5,
                stream: "cpu_usage/task".to_string(),
                as_of_ts: 1_700_000_000_000_000_000,
                blob: b"columns".to_vec(),
            },
            Frame::StreamSummary {
                source: 5,
                stream: String::new(),
                as_of_ts: -1,
                blob: Vec::new(),
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
    }
}
