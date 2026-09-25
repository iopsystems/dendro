//! Applying frames to an archive.

use std::collections::BTreeMap;

use tracing::warn;

use crate::archive::{CallerRow, SourceMeta};
use crate::error::{Error, Result};
use crate::writer::{SourceWriter, Writer};

use super::frame::{Frame, IndexKind, IndexState, NO_INDEX_STATE};

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
    /// WAL rows written.
    pub rows: usize,
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
    /// Stream summaries set.
    pub stream_summaries: usize,
    /// Sources opened.
    pub sources: usize,
    /// Whether this frame's sequence number skipped one, which means a `Rows`
    /// frame was lost in transit. The rows that did arrive are still good; see
    /// rule 6 in [the module docs](crate::replicate).
    pub gap: bool,
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
}

/// Applies replication frames to an archive.
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
/// still correct, only slower to read.
///
/// **It applies what it is handed.** There is no authentication here and no
/// check that the publisher is entitled to the source it claims; whatever
/// decides that is the transport's, which is also where the frames came from.
pub struct Subscriber {
    writer: Writer,
    sources: BTreeMap<u32, SourceState>,
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
        }
    }

    /// Apply one frame.
    pub fn apply(&mut self, frame: Frame) -> Result<Applied> {
        match frame {
            Frame::Handshake {
                source,
                uuid,
                labels,
                metadata,
                clock_anchor_wall_ns,
                complete,
            } => {
                let writer = self.writer.add_source_with_uuid(
                    SourceMeta {
                        labels,
                        metadata,
                        clock_anchor_wall_ns,
                    },
                    uuid.as_deref(),
                )?;
                self.sources.insert(
                    source,
                    SourceState {
                        writer,
                        // A source starts with no index applied, which is also
                        // what a publisher with no secondary index sends
                        // forever — so such a stream matches from the first
                        // frame and needs no index to be replicated.
                        index_state: NO_INDEX_STATE,
                        seen_full: false,
                        last_seq: None,
                        complete,
                        last_offset: None,
                        logged_gap: false,
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
                if unresolvable || index_state != st.index_state {
                    return Ok(Applied {
                        rows_skipped: rows.len(),
                        gap,
                        ..Applied::default()
                    });
                }

                let n = rows.len();
                if n > 0 {
                    st.writer.wal(rows)?;
                }
                Ok(Applied {
                    rows: n,
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
                st.last_offset = Some((ts, offset_ns));
                Ok(Applied {
                    clock_offsets: 1,
                    ..Applied::default()
                })
            }

            Frame::StreamSummary {
                source,
                stream,
                as_of_ts,
                blob,
            } => {
                let st = Self::source_mut(&mut self.sources, source)?;
                st.writer.stream_summary(stream, as_of_ts, blob)?;
                Ok(Applied {
                    stream_summaries: 1,
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
            total.segments += one.segments;
            total.segments_held += one.segments_held;
            total.index_entries += one.index_entries;
            total.clock_offsets += one.clock_offsets;
            total.sources += one.sources;
            total.gap |= one.gap;
        }
        Ok(total)
    }

    /// Seal `streams` of one source, by its handshake ordinal.
    ///
    /// The subscriber does not do this on its own — see the type's docs for
    /// why — so a caller that wants segments rather than an ever-growing WAL
    /// tail drives it, with [`SegmentAccount`](crate::seal::SegmentAccount) or
    /// any other policy.
    pub fn seal(&mut self, source: u32, streams: Vec<String>) -> Result<()> {
        Self::source_mut(&mut self.sources, source)?
            .writer
            .seal(streams)
    }

    /// Block until everything handed over so far has been committed.
    ///
    /// [`apply`](Self::apply) is fire-and-forget for rows and index entries,
    /// the same way [`SourceWriter::wal`] is, so a reader opened immediately
    /// after one may not see them yet. This is the point at which they are on
    /// disk.
    pub fn sync(&mut self) -> Result<()> {
        for st in self.sources.values_mut() {
            st.writer.sync()?;
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
    /// rather than at a timestamp this process invented.
    pub fn finish(mut self) -> Result<()> {
        for (_, st) in std::mem::take(&mut self.sources) {
            let SourceState {
                writer,
                complete,
                last_offset,
                ..
            } = st;
            if complete {
                writer.finalize(last_offset.unwrap_or((0, 0)))?;
            } else {
                drop(writer);
            }
        }
        self.writer.join()
    }

    /// The archive being written.
    pub fn path(&self) -> &std::path::Path {
        self.writer.path()
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
