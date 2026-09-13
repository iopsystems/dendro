// Drives the writer, so it needs the `write` feature.
#![cfg(feature = "write")]

//! The encoder version marker: the one thing that turns a silent
//! disagreement between the writing and reading encoder into an error.

use std::collections::BTreeMap;

use dendro::db::{Db, SourceMeta, WalRow};
use dendro::keys;
use dendro::read;
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Archive;
use dendro::Error;

struct Tags(&'static str);
impl SegmentEncoder for Tags {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        let ts: Vec<String> = rows.iter().map(|r| r.ts.to_string()).collect();
        Ok(Some(Segment {
            bytes: ts.join(",").into_bytes(),
            rows: rows.len() as u64,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
            index: None,
        }))
    }
    fn version(&self) -> Option<&str> {
        if self.0.is_empty() {
            None
        } else {
            Some(self.0)
        }
    }
}

fn source() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::new(),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

fn row(ts: i64) -> WalRow {
    WalRow {
        stream: "s".to_string(),
        ts,
        wall_offset: 0,
        row: vec![1],
    }
}

fn written_with(path: &std::path::Path, version: &'static str) -> i64 {
    let (archive, mut w) = Archive::single(path, Box::new(Tags(version)), source()).unwrap();
    let id = w.source_id();
    w.wal(vec![row(1)]).unwrap();
    archive.finalize_single(w, (1, 0)).unwrap();
    id
}

#[test]
fn the_writing_encoders_version_is_recorded_and_a_mismatch_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("e.dendro");
    let id = written_with(&path, "v1");
    let db = Db::open_read_only(&path).unwrap();
    assert_eq!(
        db.source_metadata(id)
            .unwrap()
            .get(keys::ENCODER)
            .map(String::as_str),
        Some("v1")
    );

    // The same version reads; a different one is refused on every path.
    assert!(read::read_archive(&db, &Tags("v1")).is_ok());
    let is_mismatch = |e: Error| matches!(e, Error::EncoderMismatch { wrote, reading, .. } if wrote == "v1" && reading == "v2");
    assert!(is_mismatch(
        read::read_archive(&db, &Tags("v2")).unwrap_err()
    ));
    assert!(is_mismatch(
        read::stream_segments(&db, id, "s", &Tags("v2")).unwrap_err()
    ));
    assert!(is_mismatch(
        read::probe(&db, id, "s", &Tags("v2")).unwrap_err()
    ));
    assert!(is_mismatch(
        read::stream_range(&db, id, "s", 0, 10, &Tags("v2")).unwrap_err()
    ));

    // An encoder that does not version itself is never checked.
    assert!(read::read_archive(&db, &Tags("")).is_ok());
    drop(db);

    // Nor can a different version resume the source, or copy it (the tail
    // is re-encoded on the way across).
    let mut archive = Archive::open(&path, Box::new(Tags("v2"))).unwrap();
    assert!(is_mismatch(archive.resume_source(id, 100).unwrap_err()));
    drop(archive);
    let src = Db::open_read_only(&path).unwrap();
    let mut dst = Db::create(&dir.path().join("copy.dendro")).unwrap();
    let err = dst
        .transaction(|tx| {
            dendro::rewrite::copy_sources_into(
                &src,
                tx,
                &dendro::rewrite::CopySpec::everything(),
                &Tags("v2"),
            )
        })
        .unwrap_err();
    assert!(is_mismatch(err));
}

/// A source from before the key, or written by an unversioned encoder, has
/// nothing to compare and is read by anything.
#[test]
fn an_unversioned_source_is_read_by_any_encoder() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("e.dendro");
    let id = written_with(&path, "");
    let db = Db::open_read_only(&path).unwrap();
    assert!(!db.source_metadata(id).unwrap().contains_key(keys::ENCODER));
    assert!(read::read_archive(&db, &Tags("v9")).is_ok());
}
