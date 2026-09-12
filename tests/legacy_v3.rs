//! Opening a `rezolus` `.rez` v3 archive, which names the same things
//! differently: `recordings` for `sources`, `recording_id` for `source_id`,
//! and `sampler` for `stream`.
//!
//! The fixture is built here with raw SQL rather than checked in as a binary,
//! so the schema this crate promises to read is written down in a form a
//! reader can check against the compatibility views in `db.rs`. If those views
//! and this DDL ever disagree, that is the bug this file exists to catch.

use std::collections::BTreeMap;

use dendro::db::{Db, WalRow};
use dendro::read;
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};

/// The v3 schema, verbatim. Identical to v4 in shape; only the names differ,
/// and this const is the record of exactly which ones — if it and
/// `LEGACY_VIEWS_SQL` ever disagree, that is the bug this file exists to catch.
///
/// Deliberately NOT built by calling into this crate: a fixture that shares
/// code with the thing under test cannot detect the thing under test being
/// renamed out from under it.
const V3_SCHEMA: &str = "
CREATE TABLE recordings(
  id INTEGER PRIMARY KEY,
  labels TEXT NOT NULL,
  metadata TEXT NOT NULL,
  complete INTEGER NOT NULL DEFAULT 0,
  clock_anchor_wall_ns INTEGER NOT NULL
);
CREATE TABLE segments(
  recording_id INTEGER NOT NULL REFERENCES recordings(id),
  sampler TEXT NOT NULL,
  seq INTEGER NOT NULL,
  rows INTEGER NOT NULL,
  first_ts INTEGER NOT NULL,
  last_ts INTEGER NOT NULL,
  bytes BLOB NOT NULL,
  PRIMARY KEY (recording_id, sampler, seq)
);
CREATE INDEX segments_by_time ON segments(recording_id, sampler, last_ts);
CREATE TABLE wal(
  recording_id INTEGER NOT NULL,
  sampler TEXT NOT NULL,
  ts INTEGER NOT NULL,
  wall_offset INTEGER NOT NULL,
  row BLOB NOT NULL,
  PRIMARY KEY (recording_id, sampler, ts)
);
CREATE TABLE clock_offsets(
  recording_id INTEGER NOT NULL,
  ts INTEGER NOT NULL,
  offset_ns INTEGER NOT NULL
);
CREATE TABLE schema_version(version INTEGER NOT NULL);
INSERT INTO schema_version(version) VALUES (3);
";

/// Reports the rows it was given without decoding them — enough to prove the
/// WAL tail was found and keyed correctly, which is what the views affect.
struct CountingEncoder;

impl SegmentEncoder for CountingEncoder {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(Segment {
            bytes: format!("tail:{}", rows.len()).into_bytes(),
            rows: rows.len() as u64,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
        }))
    }
}

fn write_v3_fixture(path: &std::path::Path) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(V3_SCHEMA).unwrap();
    conn.execute(
        "INSERT INTO recordings(id, labels, metadata, complete, clock_anchor_wall_ns) \
         VALUES (1, ?1, '{}', 1, 0)",
        [r#"{"source":"weather"}"#],
    )
    .unwrap();
    // One sealed segment...
    conn.execute(
        "INSERT INTO segments(recording_id, sampler, seq, rows, first_ts, last_ts, bytes) \
         VALUES (1, 'temps', 0, 2, 10, 20, ?1)",
        [b"sealed".to_vec()],
    )
    .unwrap();
    // ...and two unsealed rows past it, plus one the watermark must exclude.
    for (ts, live) in [(20u64, false), (30, true), (40, true)] {
        conn.execute(
            "INSERT INTO wal(recording_id, sampler, ts, wall_offset, row) VALUES (1, 'temps', ?1, 0, ?2)",
            rusqlite::params![ts as i64, format!("row{ts}").into_bytes()],
        )
        .unwrap();
        let _ = live;
    }
}

#[test]
fn a_legacy_v3_archive_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.rez");
    write_v3_fixture(&path);

    let db = Db::open(&path).expect("a v3 archive should open");
    let sources = read::read_archive(&db, &CountingEncoder).expect("read");
    assert_eq!(sources.len(), 1);

    let rec = &sources[0];
    assert_eq!(
        rec.labels,
        BTreeMap::from([("source".to_string(), "weather".to_string())])
    );
    assert!(rec.complete);

    let (stream, segments) = &rec.streams[0];
    assert_eq!(stream, "temps");
    // The sealed segment, then the live tail — and the tail holds the two rows
    // past `last_ts`, not all three. The watermark has to survive the views.
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0], b"sealed");
    assert_eq!(segments[1], b"tail:2");
}

/// The catalog is reachable through the views, not just the row data.
#[test]
fn a_legacy_v3_archive_answers_catalog_questions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.rez");
    write_v3_fixture(&path);

    let db = Db::open(&path).unwrap();
    assert_eq!(db.all_streams(1).unwrap(), vec!["temps".to_string()]);
    assert_eq!(db.read_segments(1, "temps").unwrap().len(), 1);
    assert_eq!(db.live_wal(1, "temps").unwrap().len(), 2);
}

/// Writing is refused, and the message says why rather than leaking SQLite's
/// `cannot modify segments because it is a view`.
#[test]
fn a_legacy_v3_archive_refuses_a_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.rez");
    write_v3_fixture(&path);

    let mut db = Db::open(&path).unwrap();
    let err = db
        .insert_wal_rows(
            1,
            &[WalRow {
                stream: "temps".to_string(),
                ts: 50,
                wall_offset: 0,
                row: b"row50".to_vec(),
            }],
        )
        .expect_err("a v3 archive must not accept a write");
    assert!(
        matches!(err, dendro::Error::ReadOnly(dendro::ReadOnly::LegacySchema)),
        "the refusal should be the typed one a caller can branch on, got: {err:?}"
    );
    assert!(
        !err.to_string().contains("because it is a view"),
        "SQLite's own message should not reach the caller, got: {err}"
    );
}

/// An unknown version is refused rather than guessed at: reading a catalog
/// under the wrong shape yields wrong data, not an error.
#[test]
fn an_unknown_schema_version_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("future.rez");
    write_v3_fixture(&path);
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("UPDATE schema_version SET version = 99", [])
        .unwrap();

    let err = match Db::open(&path) {
        Ok(_) => panic!("an unknown version must not open"),
        Err(e) => e,
    };
    assert!(
        matches!(err, dendro::Error::UnsupportedSchema { found: 99, .. }),
        "the version found must be recoverable without parsing a sentence, got: {err:?}"
    );
}

/// A legacy archive opens through the read-only handle.
///
/// This is the path documented as the way to read a live buffer, an artifact
/// you do not own, or read-only media — and a legacy `.rez` is exactly the file
/// that arrives that way. `query_only = 1` refused the `CREATE TEMP VIEW` the
/// compat path installs, so it failed on the one format it exists to serve.
#[test]
fn a_legacy_v3_archive_opens_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.rez");
    write_v3_fixture(&path);

    let db = Db::open_read_only(&path).expect("a v3 archive must open read-only");
    assert_eq!(db.all_streams(1).unwrap(), vec!["temps".to_string()]);
    assert_eq!(db.read_segments(1, "temps").unwrap().len(), 1);
    assert_eq!(db.live_wal(1, "temps").unwrap().len(), 2);

    let recordings = read::read_archive(&db, &CountingEncoder).expect("read");
    assert_eq!(recordings[0].streams[0].1.len(), 2);
}

/// A legacy archive opens from bytes — the browser-upload path.
///
/// The catalog probe looks for a table by name to tell "not an archive" from
/// "a copy taken mid-write". The rename to `sources` left it looking for a
/// table a v3 file does not have, and the compat views are installed later, so
/// every legacy upload was diagnosed as a truncated copy.
#[test]
fn a_legacy_v3_archive_opens_from_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.rez");
    write_v3_fixture(&path);

    let bytes = std::fs::read(&path).unwrap();
    let db = Db::open_bytes(bytes).expect("a v3 archive must open from bytes");
    assert_eq!(db.all_streams(1).unwrap(), vec!["temps".to_string()]);
    assert_eq!(db.read_sources().unwrap().len(), 1);
}
