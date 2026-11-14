//! What a copy holds, as the request that resumes it (WIRE.md §8).

/// What a copy holds of each source, sent to the publisher it resumes from.
///
/// The caller's transport carries it from the subscriber's side to the
/// publisher's, as it carries the frames the other way.
/// [`encode`](Self::encode) and [`decode`](Self::decode) are its bytes. Built
/// from what the copy's archive holds, so it commits with the rows it
/// describes.
///
/// Sources are in `uuid` order, and each source's streams and caller-row
/// names in name order, each name once; [`encode`](Self::encode) and
/// [`decode`](Self::decode) refuse a request that is not.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResumeRequest {
    /// The sources the copy holds, by uuid.
    pub sources: Vec<SourcePosition>,
    /// Where a source the request does not name is shipped from: a row
    /// timestamp, `i64::MIN` for its first row.
    pub since_for_new_sources: i64,
}

impl Default for ResumeRequest {
    /// No sources, and a source the request does not name shipped from its
    /// first row: the request of an empty copy, which is a full catch-up.
    fn default() -> Self {
        ResumeRequest {
            sources: Vec::new(),
            since_for_new_sources: i64::MIN,
        }
    }
}

/// One source of a [`ResumeRequest`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SourcePosition {
    /// The source's uuid (FORMAT.md §3.1).
    pub uuid: String,
    /// Each stream the copy holds a row of.
    pub streams: Vec<StreamPosition>,
    /// Each caller-row name the copy holds a row of.
    pub caller_rows: Vec<CallerRowPosition>,
}

/// One stream of a [`SourcePosition`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamPosition {
    /// The stream.
    pub stream: String,
    /// The newest row timestamp the copy holds of the stream, sealed or live.
    pub newest: i64,
    /// The copy's segments of the stream as `(first_ts, last_ts)`, newest
    /// first, no two overlapping. The list may stop before the oldest.
    pub spans: Vec<(i64, i64)>,
}

impl StreamPosition {
    /// The copy's watermark: the newest timestamp it holds sealed, or `None`
    /// when it holds no segment of the stream.
    pub fn watermark(&self) -> Option<i64> {
        self.spans.first().map(|&(_, last)| last)
    }
}

/// One caller-row name of a [`SourcePosition`]: the position of the copy's
/// newest entry.
///
/// A name's rows append in `ts` order (FORMAT.md §3.5), so the newest entry's
/// `ts` and the count of entries the copy holds at it say where the origin
/// continues.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CallerRowPosition {
    /// The caller-row name.
    pub name: String,
    /// The newest entry's timestamp.
    pub ts: i64,
    /// How many entries the copy holds at `ts`; at least 1.
    pub at_ts: u64,
}
