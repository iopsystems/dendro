// Drives the writer, so it needs the `write` feature.
#![cfg(feature = "write")]

//! Reopening an archive for append: a source continues as a new writer
//! session rather than colliding with its own past.

use std::collections::BTreeMap;

use dendro::db::{Db, SourceMeta, WalRow};
use dendro::keys;
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Archive;
use dendro::Error;

/// Reports the timestamps it was handed.
struct Tags;

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
        }))
    }
}

fn source() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), "a".to_string())]),
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

fn sessions_of(db: &Db, id: i64) -> Vec<serde_json::Value> {
    let md = db.source_metadata(id).unwrap();
    serde_json::from_str(&md[keys::WRITER_SESSIONS]).unwrap()
}

fn seqs_of(db: &Db, id: i64) -> Vec<u64> {
    db.read_segment_meta(id, "s")
        .unwrap()
        .into_iter()
        .map(|(seq, _)| seq)
        .collect()
}

/// A finalized archive reopens, a source resumes as a second session, and
/// everything continues rather than collides: `seq` picks up after the last
/// sealed segment, `complete` goes down and comes back up, and the session
/// and its discontinuity are recorded where a reader looks.
#[test]
fn a_source_resumes_and_continues_its_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.dendro");
    let (archive, mut w) = Archive::single(&path, Box::new(Tags), source()).unwrap();
    let id = w.source_id();
    for ts in [1_000, 2_000] {
        w.wal(vec![row(ts)]).unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    archive.finalize_single(w, (2_000, 0)).unwrap();
    {
        let db = Db::open_read_only(&path).unwrap();
        assert!(db.read_sources().unwrap()[0].complete);
        assert_eq!(sessions_of(&db, id).len(), 1, "one session so far");
        assert_eq!(seqs_of(&db, id), vec![0, 1]);
    }

    // A new process: new archive handle, new anchor, same source.
    let mut archive = Archive::open(&path, Box::new(Tags)).unwrap();
    let (mut w, last_ts) = archive.resume_source(id, 10_000).unwrap();
    assert_eq!(last_ts, Some(2_000));
    assert_eq!(w.floor_ts(), Some(2_000));
    {
        let db = Db::open_read_only(&path).unwrap();
        assert!(
            !db.read_sources().unwrap()[0].complete,
            "resumed: not finished"
        );
        let sessions = sessions_of(&db, id);
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[1]["clock_anchor_wall_ns"], 10_000);
        assert_eq!(sessions[1]["resumed_after_ts"], 2_000);
        assert_ne!(sessions[0]["session"], sessions[1]["session"]);
        let md = db.source_metadata(id).unwrap();
        let events: serde_json::Value = serde_json::from_str(&md[keys::EVENTS]).unwrap();
        let events = events["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["kind"], "writer_session");
        assert_eq!(events[0]["timestamp"], 10_000);
    }
    for ts in [11_000, 12_000] {
        w.wal(vec![row(ts)]).unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    archive.finalize_single(w, (12_000, 0)).unwrap();

    let db = Db::open_read_only(&path).unwrap();
    assert!(db.read_sources().unwrap()[0].complete, "finalized again");
    assert_eq!(seqs_of(&db, id), vec![0, 1, 2, 3], "the sequence continued");
    assert_eq!(db.total_rows(id, "s").unwrap(), 4);
    // Both finalize observations are in the series; the `(source_id, ts)`
    // key on clock_offsets holds across sessions.
    let offsets: Vec<i64> = db
        .read_clock_offsets(id)
        .unwrap()
        .into_iter()
        .map(|(ts, _)| ts)
        .collect();
    assert!(
        offsets.contains(&2_000) && offsets.contains(&12_000),
        "{offsets:?}"
    );
}

/// The wall clock going backwards across a restart would put the new
/// session's rows before the old session's. Refused at resume (the anchor),
/// at the handle (`wal`), and on the writer thread (`wal_tick`), never
/// written.
#[test]
fn a_resumed_source_refuses_to_run_backwards() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.dendro");
    let (archive, mut w) = Archive::single(&path, Box::new(Tags), source()).unwrap();
    let id = w.source_id();
    w.wal(vec![row(5_000)]).unwrap();
    archive.finalize_single(w, (5_000, 0)).unwrap();

    let mut archive = Archive::open(&path, Box::new(Tags)).unwrap();
    match archive.resume_source(id, 5_000).unwrap_err() {
        Error::TimelineBackwards { ts, floor, .. } => assert_eq!((ts, floor), (5_000, 5_000)),
        other => panic!("expected TimelineBackwards, got {other}"),
    }
    // Refused before anything changed.
    assert!(Db::open_read_only(&path).unwrap().read_sources().unwrap()[0].complete);

    let (mut w, _) = archive.resume_source(id, 6_000).unwrap();
    assert!(matches!(
        w.wal(vec![row(4_000)]).unwrap_err(),
        Error::TimelineBackwards {
            ts: 4_000,
            floor: 5_000,
            ..
        }
    ));
    // Through the archive-level tick, the writer drops it and stays up.
    archive.wal_tick(vec![(id, vec![row(4_500)])]).unwrap();
    w.wal(vec![row(6_001)]).unwrap();
    archive.finalize_single(w, (6_001, 0)).unwrap();
    let db = Db::open_read_only(&path).unwrap();
    let live: Vec<i64> = db
        .live_wal(id, "s")
        .unwrap()
        .into_iter()
        .map(|r| r.ts)
        .collect();
    assert_eq!(
        live,
        vec![5_000, 6_001],
        "nothing before the floor was written"
    );
}

#[test]
fn resume_refuses_a_source_that_is_not_there_and_stays_usable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.dendro");
    let (archive, w) = Archive::single(&path, Box::new(Tags), source()).unwrap();
    archive.finalize_single(w, (1, 0)).unwrap();
    let mut archive = Archive::open(&path, Box::new(Tags)).unwrap();
    let err = archive.resume_source(999, 1_000).unwrap_err();
    assert!(err.to_string().contains("no source with id 999"), "{err}");
    // The writer is still usable: a fresh source can be added.
    let w = archive.add_source(source()).unwrap();
    drop(w);
    archive.join().unwrap();
}

/// A legacy (v3) archive is readable, not writable: reopening it for append
/// is refused up front, before a writer thread exists.
#[test]
fn a_legacy_archive_cannot_be_reopened_for_append() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.rez");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE recordings(id INTEGER PRIMARY KEY, labels TEXT NOT NULL, \
         metadata TEXT NOT NULL, complete INTEGER NOT NULL DEFAULT 0, \
         clock_anchor_wall_ns INTEGER NOT NULL); \
         CREATE TABLE segments(recording_id INTEGER NOT NULL, sampler TEXT NOT NULL, \
         seq INTEGER NOT NULL, rows INTEGER NOT NULL, first_ts INTEGER NOT NULL, \
         last_ts INTEGER NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY (recording_id, sampler, seq)); \
         CREATE TABLE wal(recording_id INTEGER NOT NULL, sampler TEXT NOT NULL, ts INTEGER NOT NULL, \
         wall_offset INTEGER NOT NULL, row BLOB NOT NULL, PRIMARY KEY (recording_id, sampler, ts)); \
         CREATE TABLE clock_offsets(recording_id INTEGER NOT NULL, ts INTEGER NOT NULL, offset_ns INTEGER NOT NULL); \
         CREATE TABLE schema_version(version INTEGER NOT NULL); \
         INSERT INTO schema_version(version) VALUES (3);",
    )
    .unwrap();
    drop(conn);
    let err = Archive::open(&path, Box::new(Tags)).unwrap_err();
    assert!(
        matches!(err, Error::ReadOnly(dendro::ReadOnly::LegacySchema)),
        "{err}"
    );
}
