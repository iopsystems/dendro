//! Applying frames to an archive.

use std::collections::BTreeMap;

use tracing::warn;

use crate::archive::{Archive, CallerRow, SourceMeta};
use crate::error::{Error, Result};
use crate::writer::{SourceWriter, Writer};

use super::frame::{Frame, IndexKind, IndexState, NO_INDEX_STATE};
use super::resume::{CallerRowPosition, ResumeRequest, SourcePosition, StreamPosition};

/// The most segment spans a resume request carries per stream, newest first.
/// The publisher's sync point is normally the copy's watermark, which the
/// newest span gives; older spans matter only where segment boundaries
/// differ, and 256 spans of 16 bytes keep a request for a few thousand
/// streams far below `MAX_FRAME_BYTES`.
const RESUME_SPANS_PER_STREAM: usize = 256;

/// What one [`apply`](Subscriber::apply) did.
///
/// Returned rather than logged, the way
/// [`Evicted`](crate::archive::Evicted) and
/// [`Compacted`](crate::rewrite::Compacted) are, so a caller can tell "nothing
/// arrived" from "everything was dropped" — two outcomes that are identical
/// from the outside and mean opposite things.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub struct Applied {
    /// WAL rows handed to the writer. Like [`SourceWriter::wal`], the hand-off
    /// is not the commit: [`sync`](Subscriber::sync) is the point at which
    /// they are on disk, and a row the writer drops (one at or below its
    /// stream's newest row, sealed or live, counted in
    /// [`dropped_out_of_order`](crate::writer::SourceWriter::dropped_out_of_order),
    /// or one in a tick the writer gave up on after retries) is counted here
    /// too.
    pub rows: usize,
    /// Rows **not** handed over, because this connection already delivered a
    /// row of their stream at or after their timestamp, or because an earlier
    /// row of this frame has the same stream and timestamp: a frame delivered
    /// twice, rows a publisher sent again, or a row sent after a newer one of
    /// its stream. A repeat written to the WAL would collide with the row
    /// already there, and a collision costs the writer the whole tick it
    /// arrived in.
    pub rows_repeated: usize,
    /// Rows **not** written, because the index state they were built against
    /// is not the one this subscriber has accumulated, or because they name an
    /// index and no [`Full`](IndexKind::Full) entry has arrived yet for their
    /// source. A
    /// non-zero count is a gap in the copy, and a deliberate one: misattributing
    /// a row to the wrong slot is worse than not having it.
    pub rows_skipped: usize,
    /// Segments inserted.
    pub segments: usize,
    /// Segments **not** inserted, because this stream's newest sealed row
    /// already covers them. The normal outcome of a reconnect, not a loss.
    pub segments_held: usize,
    /// Index entries written to the caller store.
    pub index_entries: usize,
    /// Clock-drift observations recorded.
    pub clock_offsets: usize,
    /// Sources opened.
    pub sources: usize,
    /// [`SourceUpdate`](Frame::SourceUpdate) frames handed to the writer. A
    /// write that fails is logged and dropped, as
    /// [`update_metadata`](crate::writer::SourceWriter::update_metadata)'s
    /// is, and the copy keeps its previous identity until the next update. A
    /// reconnect's handshake carries the current identity into a new source
    /// row.
    pub source_updates: usize,
    /// Whether this frame's sequence number skipped one, which means a `Rows`
    /// frame was lost in transit. The rows that did arrive are still good; see
    /// rule 6 in [the module docs](crate::replicate).
    pub gap: bool,
    /// [`Sync`](Frame::Sync) frames applied, on a resumed copy.
    pub syncs: usize,
    /// Live rows at or below a sync point that a [`Sync`](Frame::Sync)
    /// deleted because the copy's encoder did not seal them. They are not in
    /// the copy, and the publisher does not send them again.
    pub rows_unsealable: usize,
    /// Rows above a sync point that a [`Sync`](Frame::Sync) deleted, live or
    /// sealed. The publisher sends again the ones it still holds.
    pub rows_replaced: usize,
    /// Live rows a [`SegmentAfter`](Frame::SegmentAfter) deleted below its
    /// segment: rows the publisher's seal pruned or discarded.
    pub rows_pruned: usize,
    /// [`Unavailable`](Frame::Unavailable) frames: spans the copy may lack
    /// rows in, because the publisher's retention removed them first.
    pub unavailable: usize,
}

/// One source of the connection, and what the subscriber has to remember about
/// it between frames.
struct SourceState {
    writer: SourceWriter,
    /// The accumulated index state: the `state` of the newest
    /// [`Index`](Frame::Index) entry applied. Compared against a `Rows` frame's
    /// own, never recomputed — the blob is opaque, so there is nothing here to
    /// compute it from, which is why it travels outside the blob.
    index_state: IndexState,
    /// Whether a `Full` entry has arrived. Before one, rows have no identity to
    /// be attributed to.
    seen_full: bool,
    /// The `seq` of the last `Rows` frame, for the gap check.
    last_seq: Option<u64>,
    /// Whether the publisher said its source was cleanly finalized, which
    /// decides whether [`finish`](Subscriber::finish) finalizes this one.
    complete: bool,
    /// The newest clock observation seen, used as the finalize observation so
    /// the copy's series ends where the publisher's did.
    last_offset: Option<(i64, i64)>,
    /// Whether a gap has already been logged for this source, so a persistently
    /// lossy transport does not produce one line per frame.
    logged_gap: bool,
    /// Per stream, the newest row timestamp handed to the writer on this
    /// connection, or held by a resumed copy, or set by a `Sync`. A row at or
    /// below it is a repeat.
    newest: BTreeMap<String, i64>,
    /// Whether this source is a resumed connection's, which applies the
    /// version 2 frames and stops when its writer drops a write.
    replica: bool,
    /// Whether a frame for this replica failed. Its later frames are refused,
    /// so nothing is written past what the failed frame left out.
    stopped: bool,
}

/// Keep each stream's newest span only, when the request would encode to
/// more than `limit` bytes. A request above `MAX_FRAME_BYTES` cannot be sent,
/// and the newest span alone gives each stream's watermark, which is the sync
/// point whenever the two sides' segment boundaries agree.
///
/// A request for more streams than one span each fits in can still exceed
/// the limit; its `encode` then refuses it.
fn fit_spans(request: &mut ResumeRequest, limit: usize) {
    if request.encoded_len() > limit {
        for source in &mut request.sources {
            for stream in &mut source.streams {
                stream.spans.truncate(1);
            }
        }
    }
}

/// A source a resumed copy holds, as its resume request described it.
struct Held {
    source_id: i64,
    /// Whether the copy holds index entries for it, so a first `Delta`
    /// continues them rather than waiting for a `Full`.
    has_index: bool,
    newest: BTreeMap<String, i64>,
}

/// Applies replication frames to an archive.
///
/// **One `Subscriber` per connection.** After a reconnect, [`finish`](Self::finish)
/// this one and open a new one: the new connection's ordinals restart at zero,
/// and a reused subscriber refuses their handshakes. A subscriber from
/// [`new`](Self::new) adds every source it is sent, so open it on a new
/// archive: an archive holds one source per uuid, and a handshake for a
/// source the archive already holds is refused as [`Error::DuplicateSource`].
/// [`resume`](Self::resume) continues the sources an archive holds.
///
/// Owns a [`Writer`], because the subscriber's archive is written the way every
/// other archive is: one writer, one file, with the checkpoint thread,
/// retention and seal machinery it already has. What arrives over the wire
/// changes what is written, not how.
///
/// **It does not seal.** dendro never seals on its own, and a subscriber is not
/// an exception: `Rows` frames land in the WAL, where they are durable and
/// readable at once, and the caller decides when a stream has accumulated
/// enough — [`seal`](Self::seal), driven by [`SealPolicy`](crate::seal::SealPolicy)
/// exactly as a local recording drives it. An archive that is never sealed is
/// still correct, only slower to read. A [`resume`](Self::resume)d subscriber
/// is the exception: it refuses `seal`, and its segments are the publisher's.
///
/// **It applies what it is handed.** There is no authentication here and no
/// check that the publisher is entitled to the source it claims; whatever
/// decides that is the transport's, which is also where the frames came from.
pub struct Subscriber {
    writer: Writer,
    sources: BTreeMap<u32, SourceState>,
    /// `Some` on a subscriber opened with [`resume`](Self::resume): the
    /// sources its request named, by uuid, until their handshakes arrive.
    resumed: Option<BTreeMap<String, Held>>,
}

impl std::fmt::Debug for Subscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscriber")
            .field("path", &self.writer.path())
            .field("sources", &self.sources.len())
            .finish_non_exhaustive()
    }
}

impl Subscriber {
    /// Subscribe into `writer`'s archive.
    ///
    /// The archive may be empty or may already hold sources; frames open their
    /// own, so an existing one is left alone.
    pub fn new(writer: Writer) -> Self {
        Subscriber {
            writer,
            sources: BTreeMap::new(),
            resumed: None,
        }
    }

    /// Resume the copy in `writer`'s archive: a subscriber for a protocol
    /// version 2 stream, and the [`ResumeRequest`] to send the publisher the
    /// copy was made from.
    ///
    /// The request names each source the archive holds by uuid, with each
    /// stream's newest row and its segment spans (the newest 256, or only
    /// the newest when that many would make the request too large to send),
    /// and each caller-row name's position. It is read from the file,
    /// so after a kill it describes what the file holds. A source with no
    /// uuid is left out, and the publisher's handshake for it adds a second
    /// source.
    ///
    /// A handshake for a source the request named continues that source
    /// without a writer session: its rows, segments and entries are the
    /// publisher's from where the copy and the publisher agree, as the
    /// frames that follow set it (WIRE.md §4.7 to §4.9). A handshake that
    /// says the publisher's source is incomplete clears a finalized copy's
    /// `complete`. Any other source is added, as [`new`](Self::new) adds it.
    ///
    /// Every source of a resumed subscriber stops at the first frame for it
    /// that fails and at the first write its writer drops for it: a tick, a
    /// row at or below its stream's newest, or an index entry below its
    /// name's newest. A `Rows` frame whose index state the copy does not hold
    /// fails, where [`new`](Self::new)'s subscriber skips its rows.
    /// [`apply`](Self::apply), [`sync`](Self::sync) and
    /// [`finish`](Self::finish) then return an error; the writer writes none
    /// of the source's rows, index entries, segments or syncs after the
    /// drop, and a new `resume` asks for the rest.
    /// Frames lost before they reach `apply` are not detected here; a
    /// version 2 [`FrameReader`](super::wire::FrameReader) refuses a stream
    /// that lost one.
    pub fn resume(writer: Writer) -> Result<(Self, ResumeRequest)> {
        let db = Archive::open(writer.path())?;
        let mut held = BTreeMap::new();
        let mut sources = Vec::new();
        for rec in db.read_sources()? {
            let Some(uuid) = rec.uuid.clone() else {
                continue;
            };
            let newest = db.held_newest_rows_of(rec.id)?;
            let mut streams = Vec::with_capacity(newest.len());
            for (stream, at) in &newest {
                let mut spans: Vec<(i64, i64)> = db
                    .read_segment_meta(rec.id, stream)?
                    .into_iter()
                    .map(|(_, m)| (m.first_ts, m.last_ts))
                    .collect();
                spans.sort_by_key(|span| std::cmp::Reverse(span.1));
                spans.truncate(RESUME_SPANS_PER_STREAM);
                streams.push(StreamPosition {
                    stream: stream.clone(),
                    newest: *at,
                    spans,
                });
            }
            let mut caller_rows = Vec::new();
            for name in db.caller_row_streams(rec.id)? {
                if let Some((ts, at_ts)) = db.caller_row_position(rec.id, &name)? {
                    caller_rows.push(CallerRowPosition { name, ts, at_ts });
                }
            }
            held.insert(
                uuid.clone(),
                Held {
                    source_id: rec.id,
                    has_index: !caller_rows.is_empty(),
                    newest,
                },
            );
            sources.push(SourcePosition {
                uuid,
                streams,
                caller_rows,
            });
        }
        sources.sort_by(|a, b| a.uuid.cmp(&b.uuid));
        let mut request = ResumeRequest {
            sources,
            ..ResumeRequest::default()
        };
        fit_spans(&mut request, super::wire::MAX_FRAME_BYTES);
        Ok((
            Subscriber {
                writer,
                sources: BTreeMap::new(),
                resumed: Some(held),
            },
            request,
        ))
    }

    /// Apply one frame.
    ///
    /// On a [`resume`](Self::resume)d subscriber, a frame that fails stops
    /// its source: every later frame for it is refused, as after a dropped
    /// write, so the copy holds nothing past the failed frame.
    pub fn apply(&mut self, frame: Frame) -> Result<Applied> {
        let ordinal = frame.source();
        let handshake = matches!(frame, Frame::Handshake { .. });
        if !handshake {
            if let Some(st) = self.sources.get(&ordinal) {
                Self::check_poisoned(ordinal, st)?;
            }
        }
        let applied = self.apply_frame(frame);
        if applied.is_err() && !handshake {
            if let Some(st) = self.sources.get_mut(&ordinal) {
                st.stopped |= st.replica;
            }
        }
        applied
    }

    fn apply_frame(&mut self, frame: Frame) -> Result<Applied> {
        match frame {
            Frame::Handshake {
                source,
                uuid,
                labels,
                metadata,
                clock_anchor_wall_ns,
                complete,
            } => {
                // Checked before the source row is written. Replacing the
                // state would send every later frame for this ordinal to the
                // new source, and the first would stop receiving with nothing
                // said.
                if self.sources.contains_key(&source) {
                    return Err(Error::Message(format!(
                        "replication handshake for ordinal {source}, which an earlier \
                         handshake on this connection already assigned; an ordinal names \
                         one source for the life of a connection"
                    )));
                }
                // The subscriber seals this source's rows with its own
                // encoder, so one that disagrees with the encoder that wrote
                // them is refused, by the rule every read path applies
                // (`segment::check_encoder`): either side reporting no version
                // is not a mismatch. Checked before the source row is written.
                if let (Some(wrote), Some(own)) = (
                    metadata.get(crate::keys::ENCODER),
                    self.writer.encoder_version(),
                ) {
                    if wrote != own {
                        return Err(Error::Message(format!(
                            "replication handshake for ordinal {source}: its source was \
                             written with encoder version {wrote:?} and this subscriber \
                             encodes with {own:?}; the rows are the encoder's bytes, so a \
                             different version is not known to decode them"
                        )));
                    }
                }
                let continued = match (&mut self.resumed, &uuid) {
                    (Some(held), Some(uuid)) => held.remove(uuid),
                    _ => None,
                };
                let replica = self.resumed.is_some();
                let (writer, seen_full, newest) = match continued {
                    Some(held) => {
                        let mut writer = self.writer.continue_replica(held.source_id)?;
                        writer.set_identity(labels, metadata)?;
                        if !complete {
                            writer.mark_incomplete()?;
                        }
                        (writer, held.has_index, held.newest)
                    }
                    None => {
                        let mut writer = self.writer.add_source_with_uuid(
                            SourceMeta {
                                labels,
                                metadata,
                                clock_anchor_wall_ns,
                            },
                            uuid.as_deref(),
                        )?;
                        if replica {
                            writer.replicate()?;
                        }
                        (writer, false, BTreeMap::new())
                    }
                };
                self.sources.insert(
                    source,
                    SourceState {
                        writer,
                        // A source starts with no index applied, which is also
                        // what a publisher with no secondary index sends
                        // forever — so such a stream matches from the first
                        // frame and needs no index to be replicated. A resumed
                        // copy that holds entries has had its `Full`.
                        index_state: NO_INDEX_STATE,
                        seen_full,
                        last_seq: None,
                        complete,
                        last_offset: None,
                        logged_gap: false,
                        newest,
                        replica,
                        stopped: false,
                    },
                );
                Ok(Applied {
                    sources: 1,
                    ..Applied::default()
                })
            }

            Frame::Index {
                source,
                stream,
                ts,
                kind,
                state,
                blob,
            } => {
                let st = Self::source_mut(&mut self.sources, source)?;
                st.writer
                    .caller_rows(stream, vec![CallerRow { ts, blob }])?;
                // The entry hashes the COMPLETE set after applying, not the
                // change, so `Full` and `Delta` both simply replace what is
                // held. That is the property that makes one comparison enough
                // and a missed `Delta` loud: the next `Rows` frame will carry a
                // state this one cannot match.
                st.index_state = state;
                if kind == IndexKind::Full {
                    st.seen_full = true;
                }
                Ok(Applied {
                    index_entries: 1,
                    ..Applied::default()
                })
            }

            Frame::Rows {
                source,
                seq,
                index_state,
                rows,
            } => {
                let st = Self::source_mut(&mut self.sources, source)?;
                let gap = match st.last_seq {
                    Some(last) => seq > last.saturating_add(1),
                    None => false,
                };
                st.last_seq = Some(seq);
                if gap && !st.logged_gap {
                    st.logged_gap = true;
                    warn!(
                        "source {source}: a replication frame was lost (seq jumped to {seq}); \
                         the rows that arrived are still good, and this is logged once"
                    );
                }

                // Rule 9, and the rule before the first `Full`. Both are the
                // same judgement: a row whose identity cannot be resolved is
                // dropped, because attributing it to the wrong slot is worse
                // than not having it.
                //
                // `NO_INDEX_STATE` is exempt from the second. It means the rows
                // were built against no index at all, which is always
                // resolvable and is what a publisher whose caller keeps no
                // secondary index sends forever — without the exemption such a
                // stream would wait for a `Full` that is never coming and drop
                // every row. A publisher that HAS an index must not declare
                // `NO_INDEX_STATE`; see [`NO_INDEX_STATE`].
                let unresolvable = index_state != NO_INDEX_STATE && !st.seen_full;
                if (unresolvable || index_state != st.index_state) && st.replica {
                    // Skipping would leave a hole below rows committed after
                    // it, which the next resume request cannot see.
                    return Err(Error::Message(format!(
                        "source {source}: a Rows frame was built against index state \
                         {index_state:?}, and the copy holds {:?}; resume it \
                         (`Subscriber::resume`) for the publisher to send them again",
                        st.index_state
                    )));
                }
                if unresolvable || index_state != st.index_state {
                    return Ok(Applied {
                        rows_skipped: rows.len(),
                        gap,
                        ..Applied::default()
                    });
                }

                // A repeat is a row at or below what earlier frames delivered
                // for its stream, or a second row at one `(stream, ts)` in
                // this frame. Rows of one stream may arrive in any order
                // within a frame, so the bound is the one before the frame.
                let offered = rows.len();
                let before = st.newest.clone();
                let mut seen: std::collections::BTreeSet<(String, i64)> = Default::default();
                let mut fresh = Vec::with_capacity(offered);
                for row in rows {
                    let delivered = before.get(&row.stream).is_some_and(|n| row.ts <= *n);
                    if delivered || !seen.insert((row.stream.clone(), row.ts)) {
                        continue;
                    }
                    let newest = st.newest.entry(row.stream.clone()).or_insert(row.ts);
                    *newest = (*newest).max(row.ts);
                    fresh.push(row);
                }
                let n = fresh.len();
                if n > 0 {
                    st.writer.wal(fresh)?;
                }
                Ok(Applied {
                    rows: n,
                    rows_repeated: offered - n,
                    gap,
                    ..Applied::default()
                })
            }

            Frame::Segment {
                source,
                stream,
                meta,
                bytes,
                caller_index,
            } => {
                let st = Self::source_mut(&mut self.sources, source)?;
                let inserted =
                    st.writer
                        .adopt_segment(&stream, &meta, &bytes, caller_index.as_deref())?;
                // A replica's rows the segment holds are repeats from here,
                // dropped here rather than by the writer, which would stop
                // the source over them.
                if st.replica {
                    let n = st.newest.entry(stream).or_insert(meta.last_ts);
                    *n = (*n).max(meta.last_ts);
                }
                Ok(Applied {
                    segments: usize::from(inserted),
                    segments_held: usize::from(!inserted),
                    ..Applied::default()
                })
            }

            Frame::ClockOffset {
                source,
                ts,
                offset_ns,
            } => {
                let st = Self::source_mut(&mut self.sources, source)?;
                st.writer.clock_offset(ts, offset_ns)?;
                // The newest by timestamp, not the last to arrive: offsets
                // are not recorded, and so not shipped, in timestamp order.
                if st.last_offset.is_none_or(|(newest, _)| ts >= newest) {
                    st.last_offset = Some((ts, offset_ns));
                }
                Ok(Applied {
                    clock_offsets: 1,
                    ..Applied::default()
                })
            }

            // `ts` is the publisher's poll time; the archive has nowhere to
            // keep it (FORMAT.md §3.1), so it is not written.
            Frame::SourceUpdate {
                source,
                ts: _,
                labels,
                metadata,
            } => {
                let st = Self::source_mut(&mut self.sources, source)?;
                st.writer.set_identity(labels, metadata)?;
                Ok(Applied {
                    source_updates: 1,
                    ..Applied::default()
                })
            }

            Frame::Sync { source, stream, at } => {
                let st = Self::replica_mut(&mut self.sources, source)?;
                let synced = st.writer.sync_to(&stream, at)?;
                // Rows above `at` arrive again, and must not read as repeats.
                st.newest.insert(stream, at);
                Ok(Applied {
                    syncs: 1,
                    rows_unsealable: synced.unsealable as usize,
                    rows_replaced: synced.replaced as usize,
                    ..Applied::default()
                })
            }

            Frame::SegmentAfter {
                source,
                stream,
                after,
                meta,
                bytes,
                caller_index,
            } => {
                let st = Self::replica_mut(&mut self.sources, source)?;
                let pruned = st.writer.adopt_segment_after(
                    &stream,
                    after,
                    &meta,
                    &bytes,
                    caller_index.as_deref(),
                )?;
                let n = st.newest.entry(stream).or_insert(meta.last_ts);
                *n = (*n).max(meta.last_ts);
                Ok(Applied {
                    segments: 1,
                    rows_pruned: pruned as usize,
                    ..Applied::default()
                })
            }

            // Changes nothing in the copy; the kind is skippable (WIRE.md §4.9).
            Frame::Unavailable {
                source,
                stream,
                after,
                before,
            } => {
                let st = Self::source_mut(&mut self.sources, source)?;
                if st.replica {
                    warn!(
                        "source {source}, stream {stream}: the publisher holds no row in \
                         ({after}, {before}), and the copy may lack rows retention removed \
                         there before they were sent"
                    );
                }
                Ok(Applied {
                    unavailable: 1,
                    ..Applied::default()
                })
            }
        }
    }

    /// Apply a batch, summing what each frame did.
    ///
    /// Stops at the first failure, having applied everything before it. There
    /// is nothing to roll back: every frame before the failure is a committed,
    /// readable part of the archive, which is what the WAL is for.
    pub fn apply_all(&mut self, frames: impl IntoIterator<Item = Frame>) -> Result<Applied> {
        let mut total = Applied::default();
        for frame in frames {
            let one = self.apply(frame)?;
            total.rows += one.rows;
            total.rows_skipped += one.rows_skipped;
            total.rows_repeated += one.rows_repeated;
            total.segments += one.segments;
            total.segments_held += one.segments_held;
            total.index_entries += one.index_entries;
            total.clock_offsets += one.clock_offsets;
            total.sources += one.sources;
            total.source_updates += one.source_updates;
            total.gap |= one.gap;
            total.syncs += one.syncs;
            total.rows_unsealable += one.rows_unsealable;
            total.rows_replaced += one.rows_replaced;
            total.rows_pruned += one.rows_pruned;
            total.unavailable += one.unavailable;
        }
        Ok(total)
    }

    /// Seal `streams` of one source, by its handshake ordinal.
    ///
    /// The subscriber does not do this on its own — see the type's docs for
    /// why — so a caller that wants segments rather than an ever-growing WAL
    /// tail drives it, with [`SegmentAccount`](crate::seal::SegmentAccount) or
    /// any other policy.
    ///
    /// Refused on a [`resume`](Self::resume)d subscriber, whose segments are
    /// the publisher's: its own seals would draw boundaries the publisher's
    /// sync points do not know.
    pub fn seal(&mut self, source: u32, streams: Vec<String>) -> Result<()> {
        let st = Self::source_mut(&mut self.sources, source)?;
        if st.replica {
            return Err(Error::Message(format!(
                "source {source} is a resumed copy, whose segments come from its publisher; \
                 it does not seal its own"
            )));
        }
        st.writer.seal(streams)
    }

    /// Block until everything handed over so far has been committed.
    ///
    /// [`apply`](Self::apply) is fire-and-forget for rows and index entries,
    /// the same way [`SourceWriter::wal`] is, so a reader opened immediately
    /// after one may not see them yet. This is the point at which they are on
    /// disk.
    ///
    /// On a [`resume`](Self::resume)d subscriber, a source whose writer
    /// dropped a write is an error here.
    pub fn sync(&mut self) -> Result<()> {
        for st in self.sources.values_mut() {
            st.writer.sync()?;
        }
        for (ordinal, st) in &self.sources {
            Self::check_poisoned(*ordinal, st)?;
        }
        Ok(())
    }

    /// Finish the archive and join its writer.
    ///
    /// A source whose handshake said the publisher's was **complete** is
    /// finalized, so the copy carries the same answer to "was this finished".
    /// One that did not is left incomplete, which is the truthful answer for a
    /// live tail: there may be data after its last row (FORMAT.md §3.1), and
    /// there is, because the publisher is still recording.
    ///
    /// The finalize observation is the newest [`ClockOffset`](Frame::ClockOffset)
    /// frame seen, so the copy's drift series ends where the publisher's did
    /// rather than at a timestamp this process invented. A source that received
    /// none is finalized with none.
    ///
    /// A resumed source that stopped, on a failed frame or a dropped write,
    /// is not finalized, and this returns its error after joining the
    /// writer: the copy has a hole, and finalizing it would say it does not.
    pub fn finish(mut self) -> Result<()> {
        let mut stopped = None;
        for (ordinal, mut st) in std::mem::take(&mut self.sources) {
            // A write still queued may be the one the writer drops.
            if st.replica {
                st.writer.sync()?;
            }
            if let Err(e) = Self::check_poisoned(ordinal, &st) {
                stopped.get_or_insert(e);
                drop(st);
                continue;
            }
            let SourceState {
                writer,
                complete,
                last_offset,
                ..
            } = st;
            if complete {
                match last_offset {
                    Some(offset) => writer.finalize(offset)?,
                    None => writer.finalize_without_offset()?,
                }
            } else {
                drop(writer);
            }
        }
        self.writer.join()?;
        stopped.map_or(Ok(()), Err)
    }

    /// The archive being written.
    pub fn path(&self) -> &std::path::Path {
        self.writer.path()
    }

    fn check_poisoned(ordinal: u32, st: &SourceState) -> Result<()> {
        if st.replica && (st.stopped || st.writer.poisoned()) {
            return Err(Error::Message(format!(
                "source {ordinal}: a frame for it failed or its writer dropped a write, so \
                 the copy may lack rows after what the file holds; finish this subscriber \
                 and resume it (`Subscriber::resume`) for the publisher to send the rest"
            )));
        }
        Ok(())
    }

    /// [`source_mut`](Self::source_mut), for a frame that acts on a resumed
    /// copy.
    fn replica_mut(
        sources: &mut BTreeMap<u32, SourceState>,
        source: u32,
    ) -> Result<&mut SourceState> {
        let st = Self::source_mut(sources, source)?;
        if !st.replica {
            return Err(Error::Message(format!(
                "replication frame for source {source} acts on a resumed copy, and this \
                 subscriber resumes none"
            )));
        }
        Ok(st)
    }

    fn source_mut(
        sources: &mut BTreeMap<u32, SourceState>,
        source: u32,
    ) -> Result<&mut SourceState> {
        sources.get_mut(&source).ok_or_else(|| {
            Error::Message(format!(
                "replication frame for source {source}, which no handshake has introduced; \
                 every frame names a source by the ordinal its handshake assigned"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(spans: usize) -> ResumeRequest {
        ResumeRequest {
            sources: vec![SourcePosition {
                uuid: "a".to_string(),
                streams: vec![StreamPosition {
                    stream: "s".to_string(),
                    newest: 1_000,
                    spans: (0..spans as i64)
                        .rev()
                        .map(|i| (i * 10, i * 10 + 5))
                        .collect(),
                }],
                caller_rows: Vec::new(),
            }],
            ..ResumeRequest::default()
        }
    }

    #[test]
    fn a_request_too_large_to_send_keeps_only_each_streams_newest_span() {
        let mut r = request(8);
        let size = r.encode().unwrap().len();
        fit_spans(&mut r, size);
        assert_eq!(r.sources[0].streams[0].spans.len(), 8, "fits as it is");
        fit_spans(&mut r, size - 1);
        assert_eq!(r.sources[0].streams[0].spans, vec![(70, 75)]);
    }
}
