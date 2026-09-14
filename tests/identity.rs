//! What makes a file an archive, and what makes a source the same source.
//!
//! The header stamp (`application_id`, `user_version`) is what lets a file be
//! recognized without opening it and refused before any pragma writes to it;
//! the source uuid is what lets two archives agree they hold the same source.

use std::collections::BTreeMap;

use dendro::archive::{sniff, sniff_bytes, Archive, ArchiveMut, Sniff, SourceMeta, WalRow};
use dendro::rewrite::{copy_sources_into, shared_sources, CopySpec};
use dendro::segment::{EncodeResult, SegmentEncoder};
use dendro::Error;

/// Never asked to encode anything here: the sources under test hold no rows.
struct Never;
impl SegmentEncoder for Never {
    fn encode(&self, _stream: &str, _rows: &[WalRow]) -> EncodeResult {
        Ok(None)
    }
}

fn source(name: &str) -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), name.to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

/// A bare SQLite database with a table of its own: what detection must not
/// accept as an archive.
fn foreign_sqlite(path: &std::path::Path) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("CREATE TABLE moz_places(id INTEGER PRIMARY KEY, url TEXT);")
        .unwrap();
}

/// Rewrite the header stamp of an archive, to model one written by another
/// build. `application_id`/`user_version` are plain header fields.
fn restamp(path: &std::path::Path, application_id: i64, user_version: i64) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.pragma_update(None, "application_id", application_id)
        .unwrap();
    conn.pragma_update(None, "user_version", user_version)
        .unwrap();
}

fn is_not_an_archive(e: &Error) -> bool {
    matches!(e, Error::NotAnArchive { .. })
}

#[test]
fn create_stamps_the_header_and_a_copy_carries_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.dendro");
    let db = ArchiveMut::create(&path).unwrap();
    // Sniffed while the creating connection is still open: the stamp has to
    // be in the archive itself, not waiting in the sidecar for a checkpoint
    // — a rolling buffer is sniffed while its writer holds it.
    assert_eq!(sniff(&path).unwrap(), Sniff::Stamped { version: 4 });
    // In memory too — the browser's report path serializes straight to bytes.
    let mem = ArchiveMut::create_in_memory().unwrap();
    assert_eq!(
        sniff_bytes(&mem.serialize().unwrap()),
        Sniff::Stamped { version: 4 }
    );
    // `VACUUM INTO` is how a live archive is copied exactly; the stamp has to
    // survive it or every such copy would open as pre-stamp and lose the
    // header gate.
    let copy = dir.path().join("copy.dendro");
    db.vacuum_into(&copy).unwrap();
    assert_eq!(sniff(&copy).unwrap(), Sniff::Stamped { version: 4 });
    Archive::open(&copy).unwrap();
}

#[test]
fn a_newer_schema_is_refused_by_name_on_every_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.dendro");
    drop(ArchiveMut::create(&path).unwrap());
    restamp(&path, i64::from(dendro::archive::APPLICATION_ID), 5);
    // Detection still says "an archive" — the sniff cannot read a version it
    // does not know — and the open is where the refusal lands.
    assert_eq!(sniff(&path).unwrap(), Sniff::Stamped { version: 5 });
    for result in [
        Archive::open(&path).map(drop),
        Archive::open(&path).map(drop),
        Archive::open_bytes(std::fs::read(&path).unwrap()).map(drop),
    ] {
        match result.unwrap_err() {
            Error::UnsupportedSchema { found, writes, .. } => {
                assert_eq!((found, writes), (5, 4));
            }
            other => panic!("expected UnsupportedSchema, got {other}"),
        }
    }
}

#[test]
fn another_applications_database_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("places.sqlite");
    foreign_sqlite(&path);
    // Unstamped, so the header cannot tell it from a pre-stamp archive; the
    // open looks for the catalog and refuses.
    assert_eq!(sniff(&path).unwrap(), Sniff::Unstamped);
    let err = Archive::open(&path).unwrap_err();
    assert!(is_not_an_archive(&err), "{err}");
    assert!(err.to_string().contains("no catalog"), "{err}");
    // And its pragmas were not rewritten on the way to that refusal.
    let conn = rusqlite::Connection::open(&path).unwrap();
    let mode: String = conn
        .pragma_query_value(None, "journal_mode", |r| r.get(0))
        .unwrap();
    assert_ne!(
        mode.to_lowercase(),
        "wal",
        "the foreign file was left alone"
    );

    // With an explicit foreign id the header alone decides.
    let stamped = dir.path().join("other.db");
    foreign_sqlite(&stamped);
    restamp(&stamped, 0x4142_4344, 7);
    assert_eq!(sniff(&stamped).unwrap(), Sniff::NotAnArchive);
    for result in [
        Archive::open(&stamped).map(drop),
        Archive::open(&stamped).map(drop),
        Archive::open_bytes(std::fs::read(&stamped).unwrap()).map(drop),
    ] {
        let err = result.unwrap_err();
        assert!(is_not_an_archive(&err), "{err}");
        assert!(err.to_string().contains("another application"), "{err}");
    }
}

#[test]
fn a_file_that_is_not_sqlite_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("notes.txt");
    std::fs::write(
        &path,
        b"not a database, and longer than a header\n".repeat(4),
    )
    .unwrap();
    assert_eq!(sniff(&path).unwrap(), Sniff::NotAnArchive);
    assert!(is_not_an_archive(&Archive::open(&path).unwrap_err()));
    assert!(is_not_an_archive(&Archive::open(&path).unwrap_err()));
    assert!(is_not_an_archive(
        &Archive::open_bytes(std::fs::read(&path).unwrap()).unwrap_err()
    ));
    // Shorter than a header is not an archive either, and not an error.
    std::fs::write(&path, b"short").unwrap();
    assert_eq!(sniff(&path).unwrap(), Sniff::NotAnArchive);
}

/// A raw copy of a live archive before its first checkpoint is SQLite's
/// header page and nothing else: no stamp, no catalog, everything in the
/// sidecar the copy did not carry. The message has to say so, because "not
/// an archive" sends the operator looking at the wrong thing.
#[test]
fn a_header_only_copy_is_named_for_what_it_is() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy.dendro");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    drop(conn);
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes.len(), 4096, "header page only");
    let err = Archive::open_bytes(bytes).unwrap_err();
    assert!(is_not_an_archive(&err), "{err}");
    assert!(err.to_string().contains("vacuum_into"), "{err}");
}

/// Every archive written before the stamp carries `application_id = 0`; the
/// `schema_version` table it has always had is what vouches for it — and is
/// still gated.
#[test]
fn a_pre_stamp_archive_still_opens_and_is_still_gated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.dendro");
    drop(ArchiveMut::create(&path).unwrap());
    restamp(&path, 0, 0);
    assert_eq!(sniff(&path).unwrap(), Sniff::Unstamped);
    Archive::open(&path).expect("opens");
    Archive::open(&path).expect("opens read-only");
    Archive::open_bytes(std::fs::read(&path).unwrap()).expect("opens from bytes");

    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("UPDATE schema_version SET version = 5", [])
        .unwrap();
    match Archive::open(&path).unwrap_err() {
        Error::UnsupportedSchema { found, .. } => assert_eq!(found, 5),
        other => panic!("expected UnsupportedSchema, got {other}"),
    }
}

#[test]
fn a_source_is_minted_a_v4_uuid_and_a_copy_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.dendro");
    let mut src = ArchiveMut::create(&src_path).unwrap();
    src.insert_source(&source("a")).unwrap();
    src.insert_source(&source("b")).unwrap();
    let ids: Vec<String> = src
        .read_sources()
        .unwrap()
        .into_iter()
        .map(|s| s.uuid.expect("minted"))
        .collect();
    assert_ne!(ids[0], ids[1], "two inserts, two identities");
    for u in &ids {
        assert_eq!(u.len(), 36, "{u}");
        assert_eq!(u.as_bytes()[14], b'4', "version nibble: {u}");
        assert!(
            matches!(u.as_bytes()[19], b'8' | b'9' | b'a' | b'b'),
            "variant: {u}"
        );
        assert!(u.chars().all(|c| c == '-' || c.is_ascii_hexdigit()), "{u}");
    }

    // A copy carries the identity verbatim, so the two archives can agree
    // they hold the same sources.
    let dst_path = dir.path().join("dst.dendro");
    let mut dst = ArchiveMut::create(&dst_path).unwrap();
    dst.transaction(|tx| copy_sources_into(&src, tx, &CopySpec::everything(), &Never))
        .unwrap();
    let copied: Vec<String> = dst
        .read_sources()
        .unwrap()
        .into_iter()
        .map(|s| s.uuid.unwrap())
        .collect();
    assert_eq!(copied, ids);
    let mut shared = shared_sources(&src, &dst).unwrap();
    shared.sort();
    let mut expected = ids.clone();
    expected.sort();
    assert_eq!(shared, expected);

    // A different archive with different sources shares nothing, even with
    // identical labels — labels are a name, the uuid is the identity.
    let other_path = dir.path().join("other.dendro");
    let mut other = ArchiveMut::create(&other_path).unwrap();
    other.insert_source(&source("a")).unwrap();
    assert!(shared_sources(&src, &other).unwrap().is_empty());
}

/// An archive from before the column: every source reads with an unknown
/// identity, and nothing else about it changes.
#[test]
fn an_archive_without_the_uuid_column_reads_unknown_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.dendro");
    drop(ArchiveMut::create(&path).unwrap());
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("ALTER TABLE sources DROP COLUMN uuid;")
        .unwrap();
    conn.execute(
        "INSERT INTO sources(labels, metadata, complete, clock_anchor_wall_ns) \
         VALUES ('{}', '{}', 1, 0)",
        [],
    )
    .unwrap();
    drop(conn);
    let db = Archive::open(&path).unwrap();
    let rows = db.read_sources().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].uuid, None);
    // And a copy of it mints a fresh identity rather than claiming sameness.
    let dst_path = dir.path().join("dst.dendro");
    let mut dst = ArchiveMut::create(&dst_path).unwrap();
    dst.transaction(|tx| copy_sources_into(&db, tx, &CopySpec::everything(), &Never))
        .unwrap();
    assert!(dst.read_sources().unwrap()[0].uuid.is_some());
    assert!(shared_sources(&db, &dst).unwrap().is_empty());
}
