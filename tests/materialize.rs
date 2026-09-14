//! One encoder contract for the three places the crate runs an encoder.
//!
//! The writer refused an over-claiming encoder at seal and a copy refused it
//! on the way across; the read path accepted it. So a reader and the next
//! seal could disagree about a live tail — the symmetry the crate depends on,
//! broken silently.

use std::collections::BTreeMap;

use dendro::archive::{ArchiveMut, SourceMeta, WalRow};
use dendro::read;
use dendro::segment::{materialize, EncodeResult, Segment, SegmentEncoder};
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
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
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
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
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

struct Panics;
impl SegmentEncoder for Panics {
    fn encode(&self, _stream: &str, _rows: &[WalRow]) -> EncodeResult {
        panic!("no");
    }
}

#[test]
fn the_shared_check_refuses_a_hole_an_overclaim_and_a_panic() {
    let input = rows(&[1, 2, 3]);
    assert!(matches!(
        materialize(&DropsMiddle, "s", &input).unwrap_err(),
        Error::EncoderContract { .. }
    ));
    assert!(matches!(
        materialize(&OverClaims, "s", &input).unwrap_err(),
        Error::EncoderContract { .. }
    ));
    match materialize(&Panics, "s", &input).unwrap_err() {
        Error::Encoder { source, .. } => assert!(source.to_string().contains("panicked: no")),
        other => panic!("{other}"),
    }
    // Empty input never reaches the encoder — a panicking one is not called.
    assert!(materialize(&Panics, "s", &[]).unwrap().is_none());
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
