//! Tailing an archive as frames.

use std::collections::{BTreeMap, BTreeSet};

use crate::archive::{Archive, SourceRow};
use crate::error::{Error, Result};

use super::frame::{Frame, IndexKind, IndexState, NO_INDEX_STATE};
use super::resume::{ResumeRequest, SourcePosition};

/// Fold one index entry into the running state.
///
/// **This is an accumulator over the entries emitted, not a hash of anything
/// decoded.** An [`ArchivePublisher`] republishes blobs the caller wrote and
/// cannot open them (FORMAT.md §3.5), so it cannot compute the caller's own
/// "hash of the live slot set". What it can do — and what rule 9 actually needs
/// — is declare a value that changes with every entry it sends, so a subscriber
/// that missed one cannot match it. A producer-side publisher, which does know
/// its slots, declares the slot-set hash instead; both are opaque to dendro,
/// and the subscriber compares rather than recomputes either way.
///
/// Two FNV-1a streams over the same bytes from different offset bases. FNV
/// because it is four lines and has no dependency; two streams because 64 bits
/// collide often enough to matter when a mismatch silently drops rows.
fn fold_index_state(state: IndexState, ts: i64, blob: &[u8]) -> IndexState {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    // The FNV offset basis, and a second basis so the two streams differ.
    const BASIS_A: u64 = 0xcbf2_9ce4_8422_2325;
    const BASIS_B: u64 = 0x9e37_79b9_7f4a_7c15;

    let mut a = if state == NO_INDEX_STATE {
        BASIS_A
    } else {
        state.0
    };
    let mut b = if state == NO_INDEX_STATE {
        BASIS_B
    } else {
        state.1
    };
    for byte in ts.to_le_bytes().iter().chain(blob) {
        a = (a ^ u64::from(*byte)).wrapping_mul(PRIME);
        b = (b ^ u64::from(*byte)).wrapping_mul(PRIME.rotate_left(17));
    }
    // A declared state must never collide with "built against no index", which
    // a subscriber exempts from the wait-for-`Full` rule. Vanishingly unlikely
    // and one comparison to rule out.
    if (a, b) == NO_INDEX_STATE {
        (1, 0)
    } else {
        (a, b)
    }
}

/// How far a stream has been published.
#[derive(Clone, Default)]
struct StreamCursor {
    /// Newest WAL row timestamp emitted, if any.
    wal_after: Option<i64>,
    /// Newest `caller_rows` timestamp emitted, and how many rows at exactly
    /// that timestamp were sent. Several may share a timestamp and they read
    /// back in insertion order (FORMAT.md §3.5), so a timestamp alone cannot
    /// say where to resume.
    index_after: Option<(i64, usize)>,
    /// On a version 2 connection, the copy's segments of the stream as
    /// `(first_ts, last_ts)`: the request's, then every one shipped, less
    /// those a `Sync` deleted. Where the publisher puts each later segment is
    /// judged against these (WIRE.md §4.7, §4.8).
    copy_spans: Vec<(i64, i64)>,
    /// On a version 2 connection, the copy's newest row of the stream, or
    /// `None` when it holds none: the `P` of each later sync point. Above
    /// the row cursor where the copy kept live rows this publisher has not
    /// shipped.
    copy_newest: Option<i64>,
}

/// One source, and everything the publisher remembers about it between polls.
#[derive(Clone)]
struct SourceCursor {
    ordinal: u32,
    source_id: i64,
    streams: BTreeMap<String, StreamCursor>,
    /// The `clock_offsets` timestamps already emitted, or held before the
    /// publisher's start point when it opened: every offset for a tailing
    /// publisher, the offsets below `since` for a catch-up. Offsets are not
    /// inserted in timestamp order: a seal records its observation at the
    /// newest row it sealed, so sealing one stream to 100 and then another
    /// to 80 inserts 80 after 100. One entry per `clock_offsets` row, pruned
    /// below the oldest the archive holds.
    clock_sent: BTreeSet<i64>,
    seq: u64,
    index_state: IndexState,
    /// Whether the opening batch has sent its `Full`. Only the first index
    /// batch is `Full`; see [`ArchivePublisher`].
    sent_full: bool,
    /// The labels and metadata last sent, in the handshake or a
    /// `SourceUpdate`, compared on each poll.
    labels: BTreeMap<String, String>,
    metadata: BTreeMap<String, String>,
}

/// Publishes an archive's contents as frames.
///
/// It reads through the ordinary read API rather than SQLite's session
/// extension. A changeset captures *physical* row changes — prunes and
/// evictions included — and would make the subscriber reproduce the publisher's
/// local `sources.id` and `segments.seq`, neither of which is an identity
/// (FORMAT.md §3.1, §3.2). Polling the catalog produces the semantic frames the
/// format is defined in.
///
/// # Two ways to start
///
/// [`catching_up`](Self::catching_up) ships sealed [`Segment`](Frame::Segment)
/// frames for everything from a timestamp onward, then tails. That is the cheap
/// backfill: a segment costs far less to ship than the rows that built it.
///
/// [`tailing`](Self::tailing) starts from the archive's present and ships only
/// what arrives after it.
///
/// **`Segment` frames come only from a source's or a stream's opening
/// batch**: the publisher's first batch, the batch in which
/// [`next`](Self::next) first sees a source created after it opened, or the
/// batch in which it first sees sealed rows of a stream with no cursor yet (a
/// stream that had no rows when a `tailing` publisher opened, no segments
/// when a `catching_up` one opened, or that did not exist then, and that was
/// sealed before a poll shipped any of its rows). After that a stream's
/// rows are shipped as [`Rows`](Frame::Rows) and the subscriber seals its
/// own; shipping the publisher's segment as well would offer the subscriber a
/// segment covering rows it already holds unsealed, which
/// [`adopt_segment`](crate::writer::SourceWriter::adopt_segment) refuses.
/// That is protocol version 1; a [`resuming`](Self::resuming) publisher ships
/// each later seal as [`SegmentAfter`](Frame::SegmentAfter) frames.
///
/// # Index entries
///
/// The first entry a source ever sends is [`Full`](IndexKind::Full) and every
/// one after it is [`Delta`](IndexKind::Delta). For an archive that is the
/// truth: the batch carrying that first entry is everything the archive holds,
/// so a subscriber that applies it holds everything the publisher does.
///
/// Keyed on having emitted an entry rather than on the opening batch having
/// finished, because an archive with no index entries yet has an **empty**
/// opening batch. Counting that as the `Full` would leave every later entry a
/// `Delta`, and a subscriber waits for a `Full` before it will attribute rows
/// to an index — so a caller that starts keeping one after the publisher
/// attached would have every row skipped for the life of the connection.
///
/// It does **not** re-emit `Full` periodically (rule 7). `caller_rows` has no
/// primary key, so re-sending entries would duplicate them in the subscriber's
/// copy rather than replace them. Rule 7 exists for eviction safety on a
/// producer-side publisher, whose subscriber can lose state to retention; an
/// archive publisher's subscriber keeps everything it was sent.
///
/// **What a late subscriber receives is bounded by what the archive still
/// holds.** Retention evicts `caller_rows` on the same cutoff as segments
/// unless the writer supplied a floor (FORMAT.md §3.5), so without one an entry
/// written once at the start of a recording is gone while rows referencing it
/// remain. The `Full` this publisher sends means
/// "everything the archive holds from the requested point", which is *complete
/// state* only if whatever wrote the archive kept it so — by writing complete
/// entries into `caller_rows` periodically. Rule 7 is that discipline, and for
/// an archive publisher it has to have been obeyed by the **writer**; nothing
/// here can reconstruct what retention removed.
///
/// Archive-to-archive copying with no retention in play is unaffected, which is
/// the case this publisher was built for.
///
/// # Identity changes and new sources
///
/// Each [`next`](Self::next) reads the archive's sources again. A source whose
/// labels or metadata differ from what was last sent gets a
/// [`SourceUpdate`](Frame::SourceUpdate) carrying both maps, ahead of its other
/// frames in that batch; its `ts` is the wall time of the poll, since the
/// archive records no time for the change. A source created after the
/// publisher opened gets a [`Handshake`](Frame::Handshake) with the next
/// ordinal, then its segments and clock offsets from the `since` of
/// [`catching_up`](Self::catching_up) (all of them for
/// [`tailing`](Self::tailing)), its index entries and its live rows, as the
/// opening catch-up ships them, then its first `Rows` frame. A change that is undone between two
/// polls is not seen.
///
/// # Polling and the seal
///
/// A tailing publisher reads the **live** WAL tail, which is by definition the
/// rows past a stream's newest sealed segment. So a seal in the publisher's
/// archive carries rows out of view, and a publisher polling more slowly than
/// the source seals would miss them. [`next`](Self::next) **detects that and
/// returns an error** naming the stream rather than shipping a stream with a
/// hole in it; the caller reconnects with `catching_up`, which is what the
/// segment frames are for. A stream with no cursor yet (one of those above)
/// has nothing to have missed; its segments are shipped instead. A
/// [`resuming`](Self::resuming) publisher ships the seal's segments, which
/// carry the rows, and does not return this error.
pub struct ArchivePublisher {
    sources: Vec<SourceCursor>,
    /// Where a source created after opening starts: the `since` of
    /// [`catching_up`](Self::catching_up), else its first row.
    since: i64,
    /// Whether this publisher serves protocol version 2, from
    /// [`resuming`](Self::resuming).
    version_2: bool,
}

impl std::fmt::Debug for ArchivePublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchivePublisher")
            .field("sources", &self.sources.len())
            .finish_non_exhaustive()
    }
}

impl ArchivePublisher {
    /// Handshake every source, then ship only what arrives after now.
    pub fn tailing(db: &Archive) -> Result<(Self, Vec<Frame>)> {
        db.read_snapshot(|db| Self::open(db, None))
    }

    /// Handshake every source, ship everything from `since` onward as sealed
    /// segments and index entries, then tail.
    ///
    /// `since` is a row timestamp, in whatever unit the archive's rows use. A
    /// segment overlapping it is shipped whole: the publisher does not open a
    /// segment, so it cannot trim one.
    pub fn catching_up(db: &Archive, since: i64) -> Result<(Self, Vec<Frame>)> {
        db.read_snapshot(|db| Self::open(db, Some(since)))
    }

    /// Answer a copy's [`ResumeRequest`] with a protocol version 2 stream:
    /// handshake every source, continue each source the request names from
    /// where the copy and this archive agree, ship every other source from
    /// the request's `since_for_new_sources`, then tail. Write the frames
    /// with a [`FrameWriter`](super::wire::FrameWriter) opened for version 2.
    ///
    /// For each stream of a named source the copy holds, the publisher picks
    /// a sync point (WIRE.md §8.2). When this archive has sealed nothing past
    /// the copy's watermark, the point is the copy's newest row and nothing
    /// is deleted. Otherwise the point is the largest value in
    /// `[L, max(L, W)]` inside no segment of either side; the stream gets a
    /// [`Sync`](Frame::Sync) to it unless it is the copy's watermark, then
    /// this archive's segments above it as
    /// [`SegmentAfter`](Frame::SegmentAfter) frames, then its live rows. A
    /// stream whose oldest row here is more than one past the copy's newest,
    /// or that retention here removed entirely after rows the copy lacks,
    /// gets an [`Unavailable`](Frame::Unavailable). A caller-row name
    /// continues after the request's position, and clock offsets are sent
    /// again from the smallest sync point.
    ///
    /// While tailing, a seal in this archive goes out as `SegmentAfter`
    /// frames, preceded by a `Sync` when the sync point is not the copy's
    /// watermark and by an `Unavailable` when retention removed rows past the
    /// copy's newest, so a seal that outruns a poll is not an error as it is
    /// on version 1.
    ///
    /// # Errors
    ///
    /// Refused when the request is ahead of this archive: a stream's newest
    /// row above every row this archive has held of it, or a caller-row
    /// position past its entries. That is an origin restored from an older
    /// backup, or a request sent to the wrong archive. Also refused when a
    /// stream has no sync point, which a copy whose segment boundaries are
    /// not this archive's can produce (WIRE.md §8.2).
    pub fn resuming(db: &Archive, request: &ResumeRequest) -> Result<(Self, Vec<Frame>)> {
        db.read_snapshot(|db| Self::open_resumed(db, request))
    }

    /// Everything new since the last call, as frames.
    ///
    /// Always emits at least one [`Rows`](Frame::Rows) frame per source, empty
    /// when nothing was observed. That is rule 6 and the keepalive: every
    /// interval produces a frame, so a gap in the sequence means a lost reading
    /// and nothing else.
    ///
    /// Index frames for a source precede its rows (rule 4), so a row can never
    /// reference identity the subscriber has not received.
    ///
    /// An interval whose rows exceed [`MAX_FRAME_BYTES`](super::wire::MAX_FRAME_BYTES)
    /// is split across several `Rows` frames with one `seq`.
    ///
    /// # Errors
    ///
    /// A call that returns `Err` leaves the publisher as it was, so a later
    /// call ships everything that call would have. On version 1 it fails when
    /// a seal has outrun a stream's cursor (see
    /// [Polling and the seal](Self#polling-and-the-seal)), and when one row,
    /// or a segment of a stream first seen sealed, makes a frame larger than
    /// `MAX_FRAME_BYTES` on its own. On version 2
    /// ([`resuming`](Self::resuming)) it fails when one row makes a frame
    /// larger than that, and when a stream has no sync point. Each of these
    /// repeats on every call until its cause is removed (the row discarded
    /// with `SourceWriter::discard_live_rows` or evicted, the segment
    /// evicted), and it holds back every source, because one call answers for
    /// all of them.
    pub fn next(&mut self, db: &Archive) -> Result<Vec<Frame>> {
        // A call that fails returns no frames, so it must not move any cursor
        // either: the frames it built for earlier sources, and the index and
        // clock entries it built for the failing one, would otherwise be
        // counted as sent and never sent. The state is a few small maps per
        // source, so it is copied rather than tracked.
        let saved = self.sources.clone();
        let result = self.next_inner(db);
        if result.is_err() {
            self.sources = saved;
        }
        result
    }

    fn next_inner(&mut self, db: &Archive) -> Result<Vec<Frame>> {
        // ONE snapshot over every read, for the reason `copy_sources_into`
        // takes one: a seal committing between two autocommit reads inserts a
        // segment the first did not see and shadows the rows the second would
        // have returned, and the seam then reads as a hole.
        db.read_snapshot(|db| {
            let mut out = Vec::new();
            let mut records = db.read_sources()?;
            let mut polled_at = None;
            for cursor in &mut self.sources {
                if let Some(rec) = records.iter().find(|r| r.id == cursor.source_id) {
                    if rec.meta.labels != cursor.labels || rec.meta.metadata != cursor.metadata {
                        let ts = match polled_at {
                            Some(ts) => ts,
                            None => *polled_at.insert(db.wall_now_ns()?),
                        };
                        cursor.labels = rec.meta.labels.clone();
                        cursor.metadata = rec.meta.metadata.clone();
                        out.push(Frame::SourceUpdate {
                            source: cursor.ordinal,
                            ts,
                            labels: cursor.labels.clone(),
                            metadata: cursor.metadata.clone(),
                        });
                    }
                }
                cursor.poll(db, self.version_2, &mut out)?;
            }

            records.retain(|r| !self.sources.iter().any(|c| c.source_id == r.id));
            if !records.is_empty() {
                let watermarks = db.sealed_watermarks()?;
                for rec in records {
                    let ordinal = next_ordinal(self.sources.len())?;
                    // Everything the new source holds was written after this
                    // publisher opened, so it is shipped from where the
                    // publisher's own catch-up started.
                    let since = Some(self.since);
                    let mut cursor = open_source(db, &rec, ordinal, since, &watermarks, &mut out)?;
                    if self.version_2 {
                        cursor.record_shipped_spans(db)?;
                    }
                    cursor.poll(db, self.version_2, &mut out)?;
                    self.sources.push(cursor);
                }
            }
            Ok(out)
        })
    }

    /// The handshake ordinal assigned to each of the archive's sources, in the
    /// order they were read.
    pub fn ordinals(&self) -> Vec<(u32, i64)> {
        self.sources
            .iter()
            .map(|c| (c.ordinal, c.source_id))
            .collect()
    }

    /// Both constructors, inside a snapshot. `since` present means catch up
    /// from there; absent means start at the present.
    fn open(db: &Archive, since: Option<i64>) -> Result<(Self, Vec<Frame>)> {
        let mut sources = Vec::new();
        let mut frames = Vec::new();
        let watermarks = db.sealed_watermarks()?;

        for (ordinal, rec) in db.read_sources()?.into_iter().enumerate() {
            let ordinal = next_ordinal(ordinal)?;
            sources.push(open_source(
                db,
                &rec,
                ordinal,
                since,
                &watermarks,
                &mut frames,
            )?);
        }

        let since = since.unwrap_or(i64::MIN);
        Ok((
            ArchivePublisher {
                sources,
                since,
                version_2: false,
            },
            frames,
        ))
    }

    /// [`resuming`](Self::resuming), inside a snapshot.
    fn open_resumed(db: &Archive, request: &ResumeRequest) -> Result<(Self, Vec<Frame>)> {
        let mut sources = Vec::new();
        let mut frames = Vec::new();
        let watermarks = db.sealed_watermarks()?;
        let since = request.since_for_new_sources;
        for (ordinal, rec) in db.read_sources()?.into_iter().enumerate() {
            let ordinal = next_ordinal(ordinal)?;
            let named = rec
                .uuid
                .as_deref()
                .and_then(|u| request.sources.iter().find(|p| p.uuid == u));
            let cursor = match named {
                Some(position) => open_resumed_source(db, &rec, ordinal, position, &mut frames)?,
                None => {
                    let mut cursor =
                        open_source(db, &rec, ordinal, Some(since), &watermarks, &mut frames)?;
                    cursor.record_shipped_spans(db)?;
                    cursor
                }
            };
            sources.push(cursor);
        }
        Ok((
            ArchivePublisher {
                sources,
                since,
                version_2: true,
            },
            frames,
        ))
    }
}

/// The largest value in `[low, high]` inside none of `spans`, where a value
/// is inside `(first, last)` when `first <= value < last`; `None` when every
/// value is.
fn sync_point(high: i64, low: i64, spans: &[(i64, i64)]) -> Option<i64> {
    let mut at = high;
    loop {
        if at < low {
            return None;
        }
        match spans
            .iter()
            .filter(|(first, last)| *first <= at && at < *last)
            .map(|(first, _)| *first)
            .min()
        {
            None => return Some(at),
            Some(first) => at = first.checked_sub(1)?,
        }
    }
}

/// One stream's oldest held row, from its segments and its live rows.
fn oldest_row(segments: &[(i64, i64)], live_first: Option<i64>) -> Option<i64> {
    segments
        .iter()
        .map(|(first, _)| *first)
        .chain(live_first)
        .min()
}

/// A named source's handshake and opening batch on a version 2 connection.
fn open_resumed_source(
    db: &Archive,
    rec: &SourceRow,
    ordinal: u32,
    position: &SourcePosition,
    frames: &mut Vec<Frame>,
) -> Result<SourceCursor> {
    let refuse = |what: String| {
        Err(Error::Message(format!(
            "source {} ({}): the resume request is ahead of this archive: {what}; it was \
             made from another archive, or this one was restored from an older copy",
            rec.id, position.uuid
        )))
    };
    let held = db.newest_rows_of(rec.id)?;
    for stream in &position.streams {
        match held.get(&stream.stream) {
            Some(newest) if stream.newest <= *newest => {}
            Some(newest) => {
                return refuse(format!(
                    "stream `{}` at {}, and this archive's newest is {newest}",
                    stream.stream, stream.newest
                ))
            }
            None => {
                return refuse(format!(
                    "stream `{}`, which this archive has never held",
                    stream.stream
                ))
            }
        }
    }
    for name in &position.caller_rows {
        let (held_ts, bound) = db.caller_row_floor(rec.id, &name.name)?;
        let newest = held_ts.max(bound);
        let count = db.caller_row_position(rec.id, &name.name)?;
        let ahead = match newest {
            None => true,
            Some(n) if name.ts > n => true,
            // At the newest held `ts`, fewer entries than the copy holds,
            // unless retention removed some there.
            Some(_) => count.is_some_and(|(ts, k)| {
                ts == name.ts && k < name.at_ts && bound.is_none_or(|b| b < ts)
            }),
        };
        if ahead {
            return refuse(format!(
                "caller-row name `{}` at ({}, {})",
                name.name, name.ts, name.at_ts
            ));
        }
    }

    frames.push(Frame::Handshake {
        source: ordinal,
        uuid: rec.uuid.clone(),
        labels: rec.meta.labels.clone(),
        metadata: rec.meta.metadata.clone(),
        clock_anchor_wall_ns: rec.meta.clock_anchor_wall_ns,
        complete: rec.complete,
    });
    let mut cursor = SourceCursor {
        ordinal,
        source_id: rec.id,
        streams: BTreeMap::new(),
        clock_sent: BTreeSet::new(),
        seq: 0,
        index_state: NO_INDEX_STATE,
        // A copy that holds entries has had its `Full` (WIRE.md §8.1).
        sent_full: !position.caller_rows.is_empty(),
        labels: rec.meta.labels.clone(),
        metadata: rec.meta.metadata.clone(),
    };

    // Index entries first (rule 4), each name after the copy's position.
    for name in db.caller_row_streams(rec.id)? {
        let after = position
            .caller_rows
            .iter()
            .find(|n| n.name == name)
            .map(|n| (n.ts, n.at_ts as usize));
        cursor.streams.entry(name.clone()).or_default().index_after = after;
        cursor.emit_index(db, &name, i64::MIN, frames)?;
    }

    let mut lowest_point = i64::MAX;
    for stream in db.all_streams(rec.id)? {
        let origin: Vec<(i64, i64)> = db
            .read_segment_meta(rec.id, &stream)?
            .into_iter()
            .map(|(_, m)| (m.first_ts, m.last_ts))
            .collect();
        let origin_watermark = origin.iter().map(|(_, last)| *last).max();
        let oldest = oldest_row(&origin, db.live_wal_span(rec.id, &stream)?.first_ts);
        let entry = cursor.streams.entry(stream.clone()).or_default();
        let Some(copy) = position.streams.iter().find(|p| p.stream == stream) else {
            // A stream the copy does not hold: from its first row.
            for segment in db.segments_overlapping(rec.id, &stream, i64::MIN, i64::MAX)? {
                entry
                    .copy_spans
                    .push((segment.meta.first_ts, segment.meta.last_ts));
                frames.push(Frame::Segment {
                    source: ordinal,
                    stream: stream.clone(),
                    meta: segment.meta,
                    bytes: segment.bytes,
                    caller_index: segment.caller_index,
                });
            }
            entry.copy_spans.sort_unstable();
            entry.wal_after = origin_watermark;
            entry.copy_newest = origin_watermark;
            lowest_point = i64::MIN;
            continue;
        };
        let newest = copy.newest;
        let watermark = copy.watermark();
        entry.copy_spans = copy.spans.iter().rev().copied().collect();
        // The fast path: nothing sealed here past the copy's watermark, so
        // the copy keeps everything and takes the rows after its newest.
        if origin_watermark.is_none_or(|wo| watermark.is_some_and(|w| wo <= w)) {
            entry.wal_after = Some(newest);
            entry.copy_newest = Some(newest);
            if let Some(o) = oldest.filter(|o| *o > newest.saturating_add(1)) {
                frames.push(Frame::Unavailable {
                    source: ordinal,
                    stream: stream.clone(),
                    after: newest,
                    before: o,
                });
            }
            lowest_point = lowest_point.min(newest);
            continue;
        }
        let at = cursor_sync(&mut cursor, db, &stream, newest, oldest, &origin, frames)?;
        lowest_point = lowest_point.min(at);
    }

    // A stream the copy holds and this archive holds nothing of: retention
    // removed it. The copy keeps its rows; the span to the newest row this
    // archive held is reported, and a later row of the stream is shipped
    // through the sync point as any other.
    let held_streams = db.all_streams(rec.id)?;
    for copy in &position.streams {
        if held_streams.contains(&copy.stream) {
            continue;
        }
        let entry = cursor.streams.entry(copy.stream.clone()).or_default();
        entry.copy_spans = copy.spans.iter().rev().copied().collect();
        entry.wal_after = Some(copy.newest);
        entry.copy_newest = Some(copy.newest);
        if let Some(bound) = held.get(&copy.stream).filter(|b| **b > copy.newest) {
            frames.push(Frame::Unavailable {
                source: ordinal,
                stream: copy.stream.clone(),
                after: copy.newest,
                before: bound.saturating_add(1),
            });
        }
        lowest_point = lowest_point.min(copy.newest);
    }

    // Clock offsets from the smallest sync point; the copy ignores one it
    // holds (`clock_offsets` is keyed by timestamp).
    cursor.clock_sent = db
        .read_clock_offsets(rec.id)?
        .into_iter()
        .map(|(ts, _)| ts)
        .filter(|ts| *ts < lowest_point)
        .collect();
    cursor.emit_clock_offsets(db, frames)?;
    Ok(cursor)
}

/// Pick a stream's sync point against the copy's spans as `cursor` holds
/// them and this archive's `origin` spans, and emit the `Sync` and the
/// `SegmentAfter` frames above it; or, when the point is the copy's
/// watermark, the `SegmentAfter` frames alone. An `Unavailable` precedes
/// them when this archive's oldest row is more than one past the copy's
/// newest. Moves the stream's row cursor to what the copy then holds, and
/// returns the point.
fn cursor_sync(
    cursor: &mut SourceCursor,
    db: &Archive,
    stream: &str,
    copy_newest: i64,
    oldest: Option<i64>,
    origin: &[(i64, i64)],
    frames: &mut Vec<Frame>,
) -> Result<i64> {
    let ordinal = cursor.ordinal;
    let source_id = cursor.source_id;
    let entry = cursor.streams.entry(stream.to_string()).or_default();
    let watermark = entry.copy_spans.iter().map(|(_, last)| *last).max();
    if let Some(o) = oldest.filter(|o| *o > copy_newest.saturating_add(1)) {
        frames.push(Frame::Unavailable {
            source: ordinal,
            stream: stream.to_string(),
            after: copy_newest,
            before: o,
        });
    }
    let low = match oldest {
        Some(o) if o <= copy_newest => o.saturating_sub(1),
        _ => copy_newest,
    };
    let high = watermark.map_or(low, |w| w.max(low));
    let spans: Vec<(i64, i64)> = entry
        .copy_spans
        .iter()
        .chain(origin.iter())
        .copied()
        .collect();
    let Some(at) = sync_point(high, low, &spans) else {
        return Err(Error::Message(format!(
            "source {source_id}: stream `{stream}` has no sync point in [{low}, {high}]: \
             every value there is inside a segment of the copy's or of this archive's, \
             which happens when the copy drew its own segment boundaries (WIRE.md §8.2)"
        )));
    };
    let synced = Some(at) != watermark;
    if synced {
        frames.push(Frame::Sync {
            source: ordinal,
            stream: stream.to_string(),
            at,
        });
        entry.copy_spans.retain(|(_, last)| *last <= at);
    }
    let mut after = at;
    let mut above: Vec<_> = db
        .segments_overlapping(source_id, stream, at.saturating_add(1), i64::MAX)?
        .into_iter()
        .filter(|s| s.meta.last_ts > at)
        .collect();
    above.sort_by_key(|s| s.meta.first_ts);
    for segment in above {
        let last = segment.meta.last_ts;
        entry.copy_spans.push((segment.meta.first_ts, last));
        frames.push(Frame::SegmentAfter {
            source: ordinal,
            stream: stream.to_string(),
            after,
            meta: segment.meta,
            bytes: segment.bytes,
            caller_index: segment.caller_index,
        });
        after = last;
    }
    // After a `Sync` the copy holds nothing above `at` but the segments;
    // without one it keeps its live rows up to its newest.
    let origin_watermark = origin.iter().map(|(_, last)| *last).max();
    let kept = if synced { at } else { copy_newest.max(at) };
    let holds = origin_watermark.map_or(kept, |w| w.max(kept));
    entry.wal_after = Some(holds);
    entry.copy_newest = Some(holds);
    Ok(at)
}

/// The ordinal for the source at `index` in the order the publisher read them.
fn next_ordinal(index: usize) -> Result<u32> {
    u32::try_from(index)
        .map_err(|_| Error::Message("an archive with more than u32::MAX sources".to_string()))
}

/// One source's handshake and opening batch, and the cursor that tails it.
/// `since` present means catch up from there; absent means start at the
/// present.
fn open_source(
    db: &Archive,
    rec: &SourceRow,
    ordinal: u32,
    since: Option<i64>,
    watermarks: &BTreeMap<i64, BTreeMap<String, i64>>,
    frames: &mut Vec<Frame>,
) -> Result<SourceCursor> {
    frames.push(Frame::Handshake {
        source: ordinal,
        uuid: rec.uuid.clone(),
        labels: rec.meta.labels.clone(),
        metadata: rec.meta.metadata.clone(),
        clock_anchor_wall_ns: rec.meta.clock_anchor_wall_ns,
        complete: rec.complete,
    });

    let mut cursor = SourceCursor {
        ordinal,
        source_id: rec.id,
        streams: BTreeMap::new(),
        clock_sent: BTreeSet::new(),
        seq: 0,
        index_state: NO_INDEX_STATE,
        sent_full: false,
        labels: rec.meta.labels.clone(),
        metadata: rec.meta.metadata.clone(),
    };

    // Index entries first, so nothing below can reference identity that
    // has not been sent (rule 4). Their own names rather than the
    // streams', because a series kept under a name no stream uses is
    // still the caller's to keep — the same rule `copy_sources_into`
    // follows.
    for name in db.caller_row_streams(rec.id)? {
        cursor.streams.entry(name.clone()).or_default();
        cursor.emit_index(db, &name, since.unwrap_or(i64::MIN), frames)?;
    }

    for stream in db.all_streams(rec.id)? {
        let entry = cursor.streams.entry(stream.clone()).or_default();
        let watermark = watermarks
            .get(&rec.id)
            .and_then(|m| m.get(&stream))
            .copied();

        match since {
            // Catch-up: every segment overlapping the window, whole.
            Some(since) => {
                for segment in db.segments_overlapping(rec.id, &stream, since, i64::MAX)? {
                    frames.push(Frame::Segment {
                        source: ordinal,
                        stream: stream.clone(),
                        meta: segment.meta,
                        bytes: segment.bytes,
                        caller_index: segment.caller_index,
                    });
                }
                // Everything up to the watermark has now been shipped,
                // as segments. Starting the cursor there rather than at
                // nothing is what lets the seal check below work the
                // same way in both modes.
                entry.wal_after = watermark;
            }
            // Tailing: start past everything the archive already holds,
            // so the first poll ships only what arrived after this.
            None => {
                let live = db.live_wal_span(rec.id, &stream)?;
                cursor
                    .streams
                    .get_mut(&stream)
                    .expect("just inserted")
                    .wal_after = live.last_ts.or(watermark);
            }
        }
    }

    // Clock offsets, bounded the same way, at open only: an offset recorded
    // later ships whatever its timestamp, as a segment sealed later does.
    // Held offsets before the start point count as sent.
    cursor.clock_sent = db
        .read_clock_offsets(rec.id)?
        .into_iter()
        .map(|(ts, _)| ts)
        .filter(|ts| since.is_none_or(|since| *ts < since))
        .collect();
    cursor.emit_clock_offsets(db, frames)?;

    Ok(cursor)
}

impl SourceCursor {
    /// On a version 2 connection, after a catch-up, record every segment
    /// this archive holds as the copy's: the catch-up shipped those at or
    /// past its `since`, and the ones below it the copy is not to receive,
    /// so the copy's watermark starts at this archive's.
    fn record_shipped_spans(&mut self, db: &Archive) -> Result<()> {
        for (stream, entry) in &mut self.streams {
            entry.copy_spans = db
                .read_segment_meta(self.source_id, stream)?
                .into_iter()
                .map(|(_, m)| (m.first_ts, m.last_ts))
                .collect();
            entry.copy_spans.sort_unstable();
            entry.copy_newest = entry.wal_after;
        }
        Ok(())
    }

    /// Emit every clock offset not yet in `clock_sent`, and forget the sent
    /// timestamps below the oldest the archive still holds, which retention
    /// removed and nothing can match again.
    fn emit_clock_offsets(&mut self, db: &Archive, out: &mut Vec<Frame>) -> Result<()> {
        let offsets = db.read_clock_offsets(self.source_id)?;
        match offsets.first() {
            Some((oldest, _)) => self.clock_sent = self.clock_sent.split_off(oldest),
            None => self.clock_sent.clear(),
        }
        for (ts, offset) in offsets {
            if !self.clock_sent.insert(ts) {
                continue;
            }
            out.push(Frame::ClockOffset {
                source: self.ordinal,
                ts,
                offset_ns: offset,
            });
        }
        Ok(())
    }

    /// Emit every index entry at or after `from` that has not been sent,
    /// advancing both the cursor and the declared state.
    fn emit_index(
        &mut self,
        db: &Archive,
        stream: &str,
        from: i64,
        out: &mut Vec<Frame>,
    ) -> Result<()> {
        let entry = self.streams.entry(stream.to_string()).or_default();
        // Resume at the last timestamp sent rather than after it: several rows
        // may share one, so the count is what says where inside it to continue.
        let (start, mut skip) = match entry.index_after {
            Some((ts, count)) => (ts, count),
            None => (from, 0),
        };
        let rows = db.read_caller_rows(self.source_id, stream, start, i64::MAX)?;
        let mut at_last: usize = 0;
        let mut last_ts: Option<i64> = entry.index_after.map(|(ts, _)| ts);
        for row in rows {
            if skip > 0 && Some(row.ts) == last_ts {
                skip -= 1;
                at_last += 1;
                continue;
            }
            if Some(row.ts) == last_ts {
                at_last += 1;
            } else {
                last_ts = Some(row.ts);
                at_last = 1;
            }
            self.index_state = fold_index_state(self.index_state, row.ts, &row.blob);
            // The first entry a source ever sends is its complete state, and
            // everything after it is a change to what the subscriber already
            // holds. Keyed on having EMITTED one, not on having finished the
            // opening batch: an archive with no index entries yet has an empty
            // opening batch, and treating that as a `Full` would leave every
            // later entry a `Delta`. A subscriber waits for a `Full` before it
            // will attribute rows to an index, so it would then skip every row
            // for the life of the connection.
            //
            // See `ArchivePublisher` for why `Full` is never re-emitted after
            // this one.
            let kind = if self.sent_full {
                IndexKind::Delta
            } else {
                IndexKind::Full
            };
            self.sent_full = true;
            out.push(Frame::Index {
                source: self.ordinal,
                stream: stream.to_string(),
                ts: row.ts,
                kind,
                state: self.index_state,
                blob: row.blob,
            });
        }
        if let Some(ts) = last_ts {
            self.streams
                .get_mut(stream)
                .expect("inserted above")
                .index_after = Some((ts, at_last));
        }
        Ok(())
    }

    /// One poll of one source: index entries, then clock offsets, then the
    /// segments of streams first seen sealed, then the interval's `Rows`
    /// frames — one, or several sharing a `seq` when the rows exceed one
    /// frame.
    fn poll(&mut self, db: &Archive, version_2: bool, out: &mut Vec<Frame>) -> Result<()> {
        for name in db.caller_row_streams(self.source_id)? {
            self.emit_index(db, &name, i64::MIN, out)?;
        }

        self.emit_clock_offsets(db, out)?;

        // The row cursors move only once the frames are built. `next` also
        // restores every cursor when a poll fails; this keeps the poll itself
        // from leaving a half-moved set if it is ever called on its own.
        let watermarks = db.sealed_watermarks()?;
        let mut segments = Vec::new();
        let mut rows = Vec::new();
        let mut cursors: Vec<(String, Option<i64>)> = Vec::new();
        for stream in db.all_streams(self.source_id)? {
            let after = self.streams.get(&stream).and_then(|e| e.wal_after);
            let watermark = watermarks
                .get(&self.source_id)
                .and_then(|m| m.get(&stream))
                .copied();

            // A seal moves rows out of the live tail. If the watermark has
            // passed the newest row this publisher shipped, the rows between
            // the two were sealed before they were read and are no longer
            // reachable here. Reporting it is the whole point: a stream with a
            // hole in it that says so can be recovered by reconnecting with
            // `catching_up`, and one that stays quiet cannot.
            //
            // `live_wal` returns exactly the rows past the watermark, so a
            // watermark AT the cursor is the ordinary case (the publisher
            // shipped those rows and the source then sealed them) and only a
            // watermark PAST it means rows went by unread.
            //
            // No row of a stream with no cursor has been shipped: it had no
            // segments when the publisher opened (or did not exist yet), and
            // no poll has read its live tail since. The subscriber holds none
            // of its rows, so its segments are shipped whole and the cursor
            // starts at the watermark.
            let mut after = after;
            // Version 2: a seal past the copy's watermark goes out as
            // `SegmentAfter` frames, after a `Sync` when needed, whatever the
            // row cursor had reached.
            let copy_newest = self.streams.get(&stream).and_then(|e| e.copy_newest);
            if version_2 && (after.is_some() || copy_newest.is_some()) {
                let origin: Vec<(i64, i64)> = db
                    .read_segment_meta(self.source_id, &stream)?
                    .into_iter()
                    .map(|(_, m)| (m.first_ts, m.last_ts))
                    .collect();
                let copy_watermark = self
                    .streams
                    .get(&stream)
                    .and_then(|e| e.copy_spans.iter().map(|(_, last)| *last).max());
                if origin
                    .iter()
                    .any(|(_, last)| copy_watermark.is_none_or(|w| *last > w))
                {
                    let copy_newest = copy_newest.max(after).max(copy_watermark).expect("checked");
                    let oldest =
                        oldest_row(&origin, db.live_wal_span(self.source_id, &stream)?.first_ts);
                    cursor_sync(
                        self,
                        db,
                        &stream,
                        copy_newest,
                        oldest,
                        &origin,
                        &mut segments,
                    )?;
                    after = self.streams.get(&stream).and_then(|e| e.wal_after);
                }
            }
            if let (Some(w), None) = (watermark, after) {
                for segment in
                    db.segments_overlapping(self.source_id, &stream, i64::MIN, i64::MAX)?
                {
                    let size = super::wire::segment_frame_bytes(
                        &stream,
                        &segment.bytes,
                        segment.caller_index.as_deref(),
                    );
                    if version_2 {
                        let entry = self.streams.entry(stream.clone()).or_default();
                        entry
                            .copy_spans
                            .push((segment.meta.first_ts, segment.meta.last_ts));
                        entry.copy_newest = entry.copy_newest.max(Some(w));
                    } else if size > super::wire::MAX_FRAME_BYTES {
                        return Err(Error::Message(format!(
                            "source {}: a segment of stream `{stream}` makes a {size}-byte \
                             replication frame, above the {}-byte limit",
                            self.source_id,
                            super::wire::MAX_FRAME_BYTES
                        )));
                    }
                    segments.push(Frame::Segment {
                        source: self.ordinal,
                        stream: stream.clone(),
                        meta: segment.meta,
                        bytes: segment.bytes,
                        caller_index: segment.caller_index,
                    });
                }
                after = Some(w);
            }
            if let (Some(w), Some(a)) = (watermark, after) {
                if w > a {
                    return Err(Error::Message(format!(
                        "source {}: stream `{stream}` sealed past this publisher's cursor \
                         (watermark {w}, last row shipped {a}); those rows are no longer \
                         in the live tail. Reconnect with `catching_up` from {a}",
                        self.source_id
                    )));
                }
            }

            let mut newest = after;
            for row in db.live_wal(self.source_id, &stream)? {
                if after.is_some_and(|a| row.ts <= a) {
                    continue;
                }
                newest = Some(newest.map_or(row.ts, |n| n.max(row.ts)));
                rows.push(row);
            }
            cursors.push((stream, newest));
        }
        // Timestamp order across the source's streams, which is the order the
        // rows were observed in and the order a subscriber's WAL wants them.
        rows.sort_by_key(|r| (r.ts, r.stream.clone()));
        let parts = split_rows(rows, super::wire::MAX_FRAME_BYTES).map_err(|size| {
            Error::Message(format!(
                "source {}: a row makes a {size}-byte replication frame on its own, above \
                 the {}-byte limit",
                self.source_id,
                super::wire::MAX_FRAME_BYTES
            ))
        })?;

        for (stream, newest) in cursors {
            let entry = self.streams.entry(stream).or_default();
            entry.wal_after = newest;
            if version_2 {
                entry.copy_newest = entry.copy_newest.max(newest);
            }
        }
        out.extend(segments);
        // Always at least one, even empty: rule 6. The empty frame is the
        // keepalive, and it is what makes a sequence gap mean a lost reading
        // rather than a quiet source. Every part of one interval carries the
        // interval's `seq`.
        for rows in parts {
            out.push(Frame::Rows {
                source: self.ordinal,
                seq: self.seq,
                index_state: self.index_state,
                rows,
            });
        }
        self.seq += 1;
        Ok(())
    }
}

/// Split one interval's rows into `Rows` frame bodies of at most `limit`
/// bytes each, in order. Always at least one part, empty when there are no
/// rows. `Err` carries the body size of a row that exceeds `limit` alone.
fn split_rows(
    rows: Vec<crate::archive::WalRow>,
    limit: usize,
) -> std::result::Result<Vec<Vec<crate::archive::WalRow>>, usize> {
    let header = super::wire::ROWS_FRAME_HEADER_BYTES;
    let mut parts = vec![Vec::new()];
    let mut size = header;
    for row in rows {
        let bytes = super::wire::rows_frame_row_bytes(&row);
        if header + bytes > limit {
            return Err(header + bytes);
        }
        if size + bytes > limit {
            parts.push(Vec::new());
            size = header;
        }
        size += bytes;
        parts.last_mut().expect("never empty").push(row);
    }
    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::split_rows;
    use crate::archive::WalRow;
    use crate::replicate::wire::{rows_frame_row_bytes, ROWS_FRAME_HEADER_BYTES};

    fn row(ts: i64, len: usize) -> WalRow {
        WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            row: vec![0; len],
        }
    }

    #[test]
    fn no_rows_is_one_empty_part() {
        assert_eq!(
            split_rows(Vec::new(), 100).unwrap(),
            vec![Vec::<WalRow>::new()]
        );
    }

    #[test]
    fn rows_split_at_the_limit_in_order() {
        let each = rows_frame_row_bytes(&row(0, 10));
        // Room for exactly two rows per part.
        let limit = ROWS_FRAME_HEADER_BYTES + 2 * each;
        let rows: Vec<WalRow> = (0..5).map(|ts| row(ts, 10)).collect();
        let parts = split_rows(rows, limit).unwrap();
        let ts: Vec<Vec<i64>> = parts
            .iter()
            .map(|p| p.iter().map(|r| r.ts).collect())
            .collect();
        assert_eq!(ts, vec![vec![0, 1], vec![2, 3], vec![4]]);
    }

    #[test]
    fn a_row_too_large_for_any_frame_is_an_error() {
        let big = row(0, 100);
        let size = ROWS_FRAME_HEADER_BYTES + rows_frame_row_bytes(&big);
        assert_eq!(
            split_rows(vec![row(1, 1), big], size - 1).unwrap_err(),
            size
        );
    }
}
