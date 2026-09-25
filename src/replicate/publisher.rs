//! Tailing an archive as frames.

use std::collections::BTreeMap;

use crate::archive::Archive;
use crate::error::{Error, Result};

use super::frame::{Frame, IndexKind, IndexState, NO_INDEX_STATE};

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
#[derive(Default)]
struct StreamCursor {
    /// Newest WAL row timestamp emitted, if any.
    wal_after: Option<i64>,
    /// Newest `caller_rows` timestamp emitted, and how many rows at exactly
    /// that timestamp were sent. Several may share a timestamp and they read
    /// back in insertion order (FORMAT.md §3.5), so a timestamp alone cannot
    /// say where to resume.
    index_after: Option<(i64, usize)>,
}

/// One source, and everything the publisher remembers about it between polls.
struct SourceCursor {
    ordinal: u32,
    source_id: i64,
    streams: BTreeMap<String, StreamCursor>,
    /// Newest `clock_offsets` timestamp emitted.
    clock_after: Option<i64>,
    seq: u64,
    index_state: IndexState,
    /// Whether the opening batch has sent its `Full`. Only the first index
    /// batch is `Full`; see [`ArchivePublisher`].
    sent_full: bool,
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
/// **Only the opening catch-up batch emits `Segment` frames.** Once tailing,
/// rows are shipped as [`Rows`](Frame::Rows) and the subscriber seals its own;
/// shipping the publisher's segment as well would offer the subscriber a
/// segment covering rows it already holds unsealed, which
/// [`adopt_segment`](crate::writer::SourceWriter::adopt_segment) refuses.
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
/// # Polling and the seal
///
/// A tailing publisher reads the **live** WAL tail, which is by definition the
/// rows past a stream's newest sealed segment. So a seal in the publisher's
/// archive carries rows out of view, and a publisher polling more slowly than
/// the source seals would miss them. [`next`](Self::next) **detects that and
/// returns an error** naming the stream rather than shipping a stream with a
/// hole in it; the caller reconnects with `catching_up`, which is what the
/// segment frames are for.
pub struct ArchivePublisher {
    sources: Vec<SourceCursor>,
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

    /// Everything new since the last call, as frames.
    ///
    /// Always emits at least one [`Rows`](Frame::Rows) frame per source, empty
    /// when nothing was observed. That is rule 6 and the keepalive: every
    /// interval produces a frame, so a gap in the sequence means a lost reading
    /// and nothing else.
    ///
    /// Index frames for a source precede its rows (rule 4), so a row can never
    /// reference identity the subscriber has not received.
    pub fn next(&mut self, db: &Archive) -> Result<Vec<Frame>> {
        // ONE snapshot over every read, for the reason `copy_sources_into`
        // takes one: a seal committing between two autocommit reads inserts a
        // segment the first did not see and shadows the rows the second would
        // have returned, and the seam then reads as a hole.
        db.read_snapshot(|db| {
            let mut out = Vec::new();
            for cursor in &mut self.sources {
                cursor.poll(db, &mut out)?;
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
            let ordinal = u32::try_from(ordinal).map_err(|_| {
                Error::Message("an archive with more than u32::MAX sources".to_string())
            })?;
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
                clock_after: None,
                seq: 0,
                index_state: NO_INDEX_STATE,
                sent_full: false,
            };

            // Index entries first, so nothing below can reference identity that
            // has not been sent (rule 4). Their own names rather than the
            // streams', because a series kept under a name no stream uses is
            // still the caller's to keep — the same rule `copy_sources_into`
            // follows.
            for name in db.caller_row_streams(rec.id)? {
                cursor.streams.entry(name.clone()).or_default();
                cursor.emit_index(db, &name, since.unwrap_or(i64::MIN), &mut frames)?;
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

            // Clock offsets, bounded the same way.
            for (ts, offset) in db.read_clock_offsets(rec.id)? {
                match since {
                    Some(since) if ts < since => continue,
                    _ => {}
                }
                if since.is_none() {
                    // Tailing: these are already in the archive's past.
                    cursor.clock_after = Some(ts);
                    continue;
                }
                frames.push(Frame::ClockOffset {
                    source: ordinal,
                    ts,
                    offset_ns: offset,
                });
                cursor.clock_after = Some(ts);
            }

            sources.push(cursor);
        }

        Ok((ArchivePublisher { sources }, frames))
    }
}

impl SourceCursor {
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

    /// One poll of one source: index entries, then clock offsets, then exactly
    /// one `Rows` frame.
    fn poll(&mut self, db: &Archive, out: &mut Vec<Frame>) -> Result<()> {
        for name in db.caller_row_streams(self.source_id)? {
            self.emit_index(db, &name, i64::MIN, out)?;
        }

        for (ts, offset) in db.read_clock_offsets(self.source_id)? {
            if self.clock_after.is_some_and(|after| ts <= after) {
                continue;
            }
            out.push(Frame::ClockOffset {
                source: self.ordinal,
                ts,
                offset_ns: offset,
            });
            self.clock_after = Some(ts);
        }

        let watermarks = db.sealed_watermarks()?;
        let mut rows = Vec::new();
        for stream in db.all_streams(self.source_id)? {
            let entry = self.streams.entry(stream.clone()).or_default();
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
            if let (Some(w), Some(after)) = (watermark, entry.wal_after) {
                if w > after {
                    return Err(Error::Message(format!(
                        "source {}: stream `{stream}` sealed past this publisher's cursor \
                         (watermark {w}, last row shipped {after}); those rows are no longer \
                         in the live tail. Reconnect with `catching_up` from {after}",
                        self.source_id
                    )));
                }
            }

            let after = entry.wal_after;
            let mut newest = after;
            for row in db.live_wal(self.source_id, &stream)? {
                if after.is_some_and(|a| row.ts <= a) {
                    continue;
                }
                newest = Some(newest.map_or(row.ts, |n| n.max(row.ts)));
                rows.push(row);
            }
            entry.wal_after = newest;
        }
        // Timestamp order across the source's streams, which is the order the
        // rows were observed in and the order a subscriber's WAL wants them.
        rows.sort_by_key(|r| (r.ts, r.stream.clone()));

        // Always, even empty: rule 6. The empty frame is the keepalive, and it
        // is what makes a sequence gap mean a lost reading rather than a quiet
        // source.
        out.push(Frame::Rows {
            source: self.ordinal,
            seq: self.seq,
            index_state: self.index_state,
            rows,
        });
        self.seq += 1;
        Ok(())
    }
}
