//! One encoder contract for the three places the crate runs an encoder.
//!
//! The writer refused an over-claiming encoder at seal and a copy refused it
//! on the way across; the read path accepted it. So a reader and the next
//! seal could disagree about a live tail — the symmetry the crate depends on,
//! broken silently.

// The archive, which the wire-only build (`--no-default-features`) leaves out.
#![cfg(feature = "archive")]

use std::collections::BTreeMap;

use dendro::archive::{ArchiveMut, SourceMeta, WalRow};
use dendro::read;
use dendro::segment::{materialize, EncodeContext, EncodeResult, Segment, SegmentEncoder};
use dendro::Error;

fn source() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::new(),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

fn rows(ts: &[i64]) -> Vec<WalRow> {
    ts.iter()
        .map(|&ts| WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            row: vec![1],
        })
        .collect()
}

/// Keeps the first and last row and drops what is between: the shape a
/// `(rows, first_ts, last_ts)` triple cannot catch and counting can.
struct DropsMiddle;
impl SegmentEncoder for DropsMiddle {
    fn encode(&self, _ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        if rows.len() < 3 {
            return Ok(None);
        }
        Ok(Some(Segment {
            bytes: b"first,last".to_vec(),
            rows: 2,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
            index: None,
        }))
    }
}

/// Claims one more row than it was given.
struct OverClaims;
impl SegmentEncoder for OverClaims {
    fn encode(&self, _ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(Segment {
            bytes: b"x".to_vec(),
            rows: rows.len() as u64 + 1,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
            index: None,
        }))
    }
}

/// Claims the right count over a span whose given end is five past a row's
/// timestamp: `[15, 30]` or `[10, 25]` from rows at 10, 20 and 30, two rows
/// either way.
struct OffRowEnd {
    first: bool,
}
impl SegmentEncoder for OffRowEnd {
    fn encode(&self, _ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        let (first_ts, last_ts) = if self.first {
            (rows[0].ts + 5, rows[2].ts)
        } else {
            (rows[0].ts, rows[1].ts + 5)
        };
        Ok(Some(Segment {
            bytes: b"x".to_vec(),
            rows: 2,
            first_ts,
            last_ts,
            index: None,
        }))
    }
}

struct Panics;
impl SegmentEncoder for Panics {
    fn encode(&self, _ctx: &EncodeContext<'_>, _rows: &[WalRow]) -> EncodeResult {
        panic!("no");
    }
}

#[test]
fn the_shared_check_refuses_a_hole_an_overclaim_an_off_row_span_and_a_panic() {
    let input = rows(&[1, 2, 3]);
    let spaced = rows(&[10, 20, 30]);
    for first in [true, false] {
        let err =
            materialize(&OffRowEnd { first }, &EncodeContext::new(1, "s"), &spaced).unwrap_err();
        assert!(
            matches!(&err, Error::EncoderContract { detail, .. }
                if detail.contains(if first { "first_ts 15" } else { "last_ts 25" })),
            "{err}"
        );
    }
    assert!(matches!(
        materialize(&DropsMiddle, &EncodeContext::new(1, "s"), &input).unwrap_err(),
        Error::EncoderContract { .. }
    ));
    assert!(matches!(
        materialize(&OverClaims, &EncodeContext::new(1, "s"), &input).unwrap_err(),
        Error::EncoderContract { .. }
    ));
    match materialize(&Panics, &EncodeContext::new(1, "s"), &input).unwrap_err() {
        Error::Encoder { source, .. } => assert!(source.to_string().contains("panicked: no")),
        other => panic!("{other}"),
    }
    // Empty input never reaches the encoder — a panicking one is not called.
    assert!(materialize(&Panics, &EncodeContext::new(1, "s"), &[])
        .unwrap()
        .is_none());
}

/// The read path runs the same check: an encoder the seal would refuse is
/// refused when a reader materializes a live tail, rather than handing back
/// bytes the next seal will disagree with.
#[test]
fn a_reader_refuses_what_the_seal_would_refuse() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.dendro");
    let mut db = ArchiveMut::create(&path).unwrap();
    let id = db.insert_source(&source()).unwrap();
    db.insert_wal_rows(id, &rows(&[1, 2, 3])).unwrap();

    let err = read::stream_segments(&db, id, "s", &DropsMiddle).unwrap_err();
    assert!(matches!(err, Error::EncoderContract { .. }), "{err}");
    let err = read::read_archive(&db, &OverClaims).unwrap_err();
    assert!(matches!(err, Error::EncoderContract { .. }), "{err}");
}
