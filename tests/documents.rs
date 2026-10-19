// The archive half; the writer tests below also need `write`.
#![cfg(feature = "archive")]

//! Archive-level documents (FORMAT.md §3.6): append-only revisions keyed by
//! `(kind, name)`, carried by every copy, readable from archives written
//! before the table existed.

use std::collections::BTreeMap;
use std::path::Path;

use dendro::archive::{Archive, ArchiveMut, Revision, SourceMeta, WalRow};
use dendro::rewrite::{copy_documents_into, copy_sources_into, CopySpec};
use dendro::segment::{EncodeResult, SegmentEncoder};
use dendro::Error;

struct Never;
impl SegmentEncoder for Never {
    fn encode(&self, _stream: &str, _rows: &[WalRow]) -> EncodeResult {
        Ok(None)
    }
}

const KIND: &str = "app.view";
const SCHEMA: &str = "app.view/1";

fn source() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("host".to_string(), "a".to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

/// The `(uuid, body)` of every revision of `(kind, name)`, oldest first;
/// a deletion's body is `None`. What a copy has to carry, without the ids it
/// renumbers.
fn revisions(db: &Archive, kind: &str, name: &str) -> Vec<(String, Option<String>)> {
    db.document_history(kind, name)
        .unwrap()
        .into_iter()
        .map(|r| match r {
            Revision::Put(d) => (d.uuid, Some(d.body)),
            Revision::Delete(d) => (d.uuid, None),
            _ => unreachable!("a revision this test does not know"),
        })
        .collect()
}

/// Each revision's `supersedes`, as the uuid of the revision it names.
/// Comparable across files, where the ids are not.
fn chain(db: &Archive, kind: &str, name: &str) -> Vec<Option<String>> {
    let history = db.document_history(kind, name).unwrap();
    let uuid_of = |id: i64| {
        history
            .iter()
            .find_map(|r| match r {
                Revision::Put(d) if d.id == id => Some(d.uuid.clone()),
                Revision::Delete(d) if d.id == id => Some(d.uuid.clone()),
                _ => None,
            })
            .expect("supersedes names a revision of the same document")
    };
    history
        .iter()
        .map(|r| r.supersedes().map(uuid_of))
        .collect()
}

/// An archive as the previous release left it: the schema minus the
/// `documents` table, which is the only difference between the two.
fn previous_release_archive(path: &Path) {
    let mut db = ArchiveMut::create(path).unwrap();
    db.insert_source(&source()).unwrap();
    drop(db);
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.execute_batch("DROP TABLE documents").unwrap();
    let left: i64 = raw
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'documents%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(left, 0, "the table and its index are gone");
}

#[test]
fn a_put_supersedes_the_current_revision_and_keeps_the_old_one() {
    let mut db = ArchiveMut::create_in_memory().unwrap();
    assert_eq!(db.document(KIND, "main").unwrap(), None);

    let v1 = db.put_document(KIND, "main", SCHEMA, r#"{"v":1}"#).unwrap();
    let first = db.document(KIND, "main").unwrap().unwrap();
    assert_eq!(
        (first.id, first.supersedes, first.body.as_str()),
        (v1, None, r#"{"v":1}"#)
    );
    assert_eq!((first.kind.as_str(), first.name.as_str()), (KIND, "main"));
    assert_eq!(first.schema, SCHEMA);

    let v2 = db.put_document(KIND, "main", SCHEMA, r#"{"v":2}"#).unwrap();
    let current = db.document(KIND, "main").unwrap().unwrap();
    assert_eq!(
        (current.id, current.supersedes, current.body.as_str()),
        (v2, Some(v1), r#"{"v":2}"#)
    );
    assert_ne!(current.uuid, first.uuid);
    // Stamped with the wall clock: after 2020, and not after the second.
    assert!(first.created_wall_ns > 1_577_836_800_000_000_000);
    assert!(first.created_wall_ns <= current.created_wall_ns);

    let history = db.document_history(KIND, "main").unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0], Revision::Put(first));
    assert_eq!(history[1], Revision::Put(current));
}

#[test]
fn listing_shows_each_current_document_of_a_kind_once() {
    let mut db = ArchiveMut::create_in_memory().unwrap();
    db.put_document(KIND, "b", SCHEMA, "1").unwrap();
    db.put_document(KIND, "a", SCHEMA, "1").unwrap();
    db.put_document(KIND, "a", SCHEMA, "2").unwrap();
    db.put_document(KIND, "gone", SCHEMA, "1").unwrap();
    db.delete_document(KIND, "gone").unwrap();
    db.put_document("app.event", "deploy", "app.event/1", "{}")
        .unwrap();
    db.put_document("app.empty", "x", "app.empty/1", "{}")
        .unwrap();
    db.delete_document("app.empty", "x").unwrap();

    let listed: Vec<(String, String)> = db
        .documents(KIND)
        .unwrap()
        .into_iter()
        .map(|d| (d.name, d.body))
        .collect();
    assert_eq!(
        listed,
        vec![
            ("a".to_string(), "2".to_string()),
            ("b".to_string(), "1".to_string())
        ]
    );
    // A kind whose every document is deleted is not listed.
    assert_eq!(db.document_kinds().unwrap(), vec!["app.event", KIND]);
}

#[test]
fn a_delete_is_a_revision_and_the_history_survives_it() {
    let mut db = ArchiveMut::create_in_memory().unwrap();
    // Nothing to delete: nothing written.
    assert_eq!(db.delete_document(KIND, "main").unwrap(), None);
    assert!(db.document_history(KIND, "main").unwrap().is_empty());

    let v1 = db.put_document(KIND, "main", SCHEMA, "1").unwrap();
    let gone = db.delete_document(KIND, "main").unwrap().unwrap();
    assert_eq!(db.document(KIND, "main").unwrap(), None);
    let history = db.document_history(KIND, "main").unwrap();
    assert_eq!(history.len(), 2);
    match &history[1] {
        Revision::Delete(d) => assert_eq!((d.id, d.supersedes), (gone, Some(v1))),
        other => panic!("expected a deletion, got {other:?}"),
    }

    // Deleting a deleted document writes nothing.
    assert_eq!(db.delete_document(KIND, "main").unwrap(), None);
    assert_eq!(db.document_history(KIND, "main").unwrap().len(), 2);

    // A put after a delete continues the chain from the deletion.
    let v3 = db.put_document(KIND, "main", SCHEMA, "3").unwrap();
    let current = db.document(KIND, "main").unwrap().unwrap();
    assert_eq!((current.id, current.supersedes), (v3, Some(gone)));
}

#[test]
fn a_conditional_put_is_refused_when_another_revision_landed() {
    let mut db = ArchiveMut::create_in_memory().unwrap();
    // `None` means "there is no current document".
    let v1 = db
        .put_document_if_current(KIND, "main", SCHEMA, "1", None)
        .unwrap();
    match db.put_document_if_current(KIND, "main", SCHEMA, "0", None) {
        Err(Error::DocumentConflict {
            expected, current, ..
        }) => assert_eq!((expected, current), (None, Some(v1))),
        other => panic!("expected a conflict, got {other:?}"),
    }

    // Two editors read v1; the first saves, the second is refused.
    let v2 = db
        .put_document_if_current(KIND, "main", SCHEMA, "2", Some(v1))
        .unwrap();
    match db.put_document_if_current(KIND, "main", SCHEMA, "99", Some(v1)) {
        Err(Error::DocumentConflict {
            kind,
            name,
            expected,
            current,
        }) => {
            assert_eq!((kind.as_str(), name.as_str()), (KIND, "main"));
            assert_eq!((expected, current), (Some(v1), Some(v2)));
        }
        other => panic!("expected a conflict, got {other:?}"),
    }
    assert_eq!(db.document(KIND, "main").unwrap().unwrap().body, "2");
    assert_eq!(db.document_history(KIND, "main").unwrap().len(), 2);

    // After a delete there is no current document, so `None` matches.
    db.delete_document(KIND, "main").unwrap();
    db.put_document_if_current(KIND, "main", SCHEMA, "4", None)
        .unwrap();
}

#[test]
fn malformed_and_reserved_documents_are_refused_and_nothing_is_written() {
    let mut db = ArchiveMut::create_in_memory().unwrap();
    let refused = [
        ("", "main", SCHEMA, "{}"),
        (KIND, "", SCHEMA, "{}"),
        ("dendro.view", "main", SCHEMA, "{}"),
        (KIND, "main", "", "{}"),
        (KIND, "main", SCHEMA, "{not json"),
        (KIND, "main", SCHEMA, ""),
    ];
    for (kind, name, schema, body) in refused {
        let got = db.put_document(kind, name, schema, body);
        assert!(
            matches!(got, Err(Error::Message(_))),
            "{kind:?}/{name:?} schema {schema:?} body {body:?}: {got:?}"
        );
    }
    assert!(db.document_kinds().unwrap().is_empty());
    assert!(db
        .document_history("dendro.view", "main")
        .unwrap()
        .is_empty());
    assert!(db.document_history(KIND, "main").unwrap().is_empty());
}

#[test]
fn a_document_rolls_back_with_its_transaction() {
    let mut db = ArchiveMut::create_in_memory().unwrap();
    let failed: dendro::Result<()> = db.transaction(|tx| {
        tx.insert_source(&source())?;
        tx.put_document(KIND, "main", SCHEMA, "{}")?;
        Err("the assembly failed".into())
    });
    assert!(failed.is_err());
    assert_eq!(db.document(KIND, "main").unwrap(), None);
    assert!(db.read_sources().unwrap().is_empty());
}

#[test]
fn documents_survive_reopening_and_the_exact_copies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.dendro");
    let mut db = ArchiveMut::create(&path).unwrap();
    db.put_document(KIND, "main", SCHEMA, "1").unwrap();
    db.put_document(KIND, "main", SCHEMA, "2").unwrap();
    drop(db);

    let reader = Archive::open(&path).unwrap();
    let want = revisions(&reader, KIND, "main");
    assert_eq!(want.len(), 2);

    let dump = dir.path().join("dump.dendro");
    reader.vacuum_into(&dump).unwrap();
    assert_eq!(
        revisions(&Archive::open(&dump).unwrap(), KIND, "main"),
        want
    );

    let bytes = reader.serialize().unwrap();
    assert_eq!(
        revisions(&Archive::open_bytes(bytes).unwrap(), KIND, "main"),
        want
    );

    // A sound archive, by the archive's own check.
    assert!(reader
        .verify(dendro::archive::Depth::Full)
        .unwrap()
        .is_sound());
}

#[test]
fn a_copy_carries_every_revision_with_its_identity() {
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.dendro");
    let mut src = ArchiveMut::create(&src_path).unwrap();
    src.insert_source(&source()).unwrap();
    src.put_document(KIND, "main", SCHEMA, "1").unwrap();
    src.put_document(KIND, "main", SCHEMA, "2").unwrap();
    src.delete_document(KIND, "main").unwrap();
    src.put_document(KIND, "main", SCHEMA, "4").unwrap();
    src.put_document("app.event", "e", "app.event/1", "{}")
        .unwrap();
    drop(src);
    let src = Archive::open(&src_path).unwrap();

    // The destination already holds a document, so its ids and the
    // source's do not line up: `supersedes` must be renumbered.
    let mut dst = ArchiveMut::create_in_memory().unwrap();
    dst.put_document("app.other", "x", "app.other/1", "{}")
        .unwrap();
    dst.put_document("app.other", "x", "app.other/1", "{}")
        .unwrap();
    // A stream filter and a time bound that select nothing still carry the
    // documents: they belong to the archive, not to a stream or a time.
    let none = |_: &str| false;
    let spec = CopySpec {
        start: 5,
        end: 6,
        keep_streams: Some(&none),
        ..CopySpec::everything()
    };
    dst.transaction(|tx| copy_sources_into(&src, tx, &spec, &Never))
        .unwrap();

    assert_eq!(revisions(&dst, KIND, "main"), revisions(&src, KIND, "main"));
    assert_eq!(chain(&dst, KIND, "main"), chain(&src, KIND, "main"));
    assert_eq!(
        revisions(&dst, "app.event", "e"),
        revisions(&src, "app.event", "e")
    );
    assert_eq!(dst.document(KIND, "main").unwrap().unwrap().body, "4");
    assert_ne!(
        dst.document(KIND, "main").unwrap().unwrap().id,
        src.document(KIND, "main").unwrap().unwrap().id,
        "the copy renumbered, or the supersedes check above proves nothing"
    );

    // Copying the same archive again adds nothing.
    let added = dst.transaction(|tx| copy_documents_into(&src, tx)).unwrap();
    assert_eq!(added, 0);
    assert_eq!(dst.document_history(KIND, "main").unwrap().len(), 4);
}

#[test]
fn two_edited_copies_combine_into_one_history_with_the_last_input_current() {
    let mut base = ArchiveMut::create_in_memory().unwrap();
    base.put_document(KIND, "main", SCHEMA, r#""base""#)
        .unwrap();
    let bytes = base.serialize().unwrap();

    // Two copies of one archive, each edited on its own.
    let edit = |body: &str| {
        let mut copy = ArchiveMut::create_in_memory().unwrap();
        let from = Archive::open_bytes(bytes.clone()).unwrap();
        copy.transaction(|tx| copy_documents_into(&from, tx))
            .unwrap();
        copy.put_document(KIND, "main", SCHEMA, body).unwrap();
        Archive::open_bytes(copy.serialize().unwrap()).unwrap()
    };
    let a = edit(r#""a""#);
    let b = edit(r#""b""#);

    let mut combined = ArchiveMut::create_in_memory().unwrap();
    combined
        .transaction(|tx| {
            copy_documents_into(&a, tx)?;
            copy_documents_into(&b, tx)
        })
        .unwrap();
    // The shared base once, then each edit; the input copied last is current.
    let history = revisions(&combined, KIND, "main");
    let bodies: Vec<Option<&str>> = history.iter().map(|(_, b)| b.as_deref()).collect();
    assert_eq!(
        bodies,
        vec![Some(r#""base""#), Some(r#""a""#), Some(r#""b""#)]
    );
    assert_eq!(
        combined.document(KIND, "main").unwrap().unwrap().body,
        r#""b""#
    );
    // A fork: both edits supersede the base.
    let chain = chain(&combined, KIND, "main");
    assert_eq!(chain[1], Some(history[0].0.clone()));
    assert_eq!(chain[2], Some(history[0].0.clone()));
}

#[test]
fn an_archive_from_before_the_table_reads_as_empty_and_gains_it_on_first_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.dendro");
    previous_release_archive(&path);

    // The read handle cannot create the table, and does not need it.
    let reader = Archive::open(&path).unwrap();
    assert_eq!(reader.document(KIND, "main").unwrap(), None);
    assert!(reader.documents(KIND).unwrap().is_empty());
    assert!(reader.document_kinds().unwrap().is_empty());
    assert!(reader.document_history(KIND, "main").unwrap().is_empty());
    drop(reader);

    let mut db = ArchiveMut::open(&path).unwrap();
    // A delete on an archive with no table is a no-op, not a schema change.
    assert_eq!(db.delete_document(KIND, "main").unwrap(), None);
    db.put_document(KIND, "main", SCHEMA, "1").unwrap();
    drop(db);

    let reader = Archive::open(&path).unwrap();
    assert_eq!(reader.document(KIND, "main").unwrap().unwrap().body, "1");
    assert_eq!(
        reader.read_sources().unwrap().len(),
        1,
        "the source is kept"
    );
    assert!(reader
        .verify(dendro::archive::Depth::Full)
        .unwrap()
        .is_sound());
}

#[test]
fn copies_to_and_from_an_archive_without_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let old = dir.path().join("old.dendro");
    previous_release_archive(&old);

    // Out of it: nothing to carry, and no table created in the source.
    let mut fresh = ArchiveMut::create_in_memory().unwrap();
    let from_old = Archive::open(&old).unwrap();
    let copied = fresh
        .transaction(|tx| copy_documents_into(&from_old, tx))
        .unwrap();
    assert_eq!(copied, 0);
    drop(from_old);

    // Into it: the table is created inside the copy's transaction.
    let mut src = ArchiveMut::create_in_memory().unwrap();
    src.put_document(KIND, "main", SCHEMA, "1").unwrap();
    let src = Archive::open_bytes(src.serialize().unwrap()).unwrap();
    let mut db = ArchiveMut::open(&old).unwrap();
    let copied = db.transaction(|tx| copy_documents_into(&src, tx)).unwrap();
    assert_eq!(copied, 1);
    assert_eq!(db.document(KIND, "main").unwrap().unwrap().body, "1");
}

#[cfg(feature = "write")]
mod during_a_recording {
    use super::*;
    use dendro::writer::Writer;

    fn row(ts: i64) -> WalRow {
        WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            row: vec![1],
        }
    }

    #[test]
    fn a_writer_stores_documents_that_readers_see_while_it_runs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live.dendro");
        let mut archive = Writer::create(&path, Box::new(Never)).unwrap();
        let mut w = archive.add_source(source()).unwrap();
        w.wal(vec![row(1_000)]).unwrap();

        // The exclusive write handle cannot have the file while the writer
        // does; the writer is the way in.
        match ArchiveMut::open(&path) {
            Err(Error::InUse { .. }) => {}
            other => panic!("expected InUse, got {other:?}"),
        }

        let v1 = archive
            .put_document(KIND, "main", SCHEMA, r#"{"v":1}"#)
            .unwrap();
        let reader = Archive::open(&path).unwrap();
        assert_eq!(reader.document(KIND, "main").unwrap().unwrap().id, v1);

        // A conflict is the caller's answer, and the writer keeps going.
        match archive.put_document_if_current(KIND, "main", SCHEMA, "{}", None) {
            Err(Error::DocumentConflict { current, .. }) => assert_eq!(current, Some(v1)),
            other => panic!("expected a conflict, got {other:?}"),
        }
        assert!(matches!(
            archive.put_document("dendro.x", "main", SCHEMA, "{}"),
            Err(Error::Message(_))
        ));
        let v2 = archive
            .put_document_if_current(KIND, "main", SCHEMA, r#"{"v":2}"#, Some(v1))
            .unwrap();
        w.wal(vec![row(2_000)]).unwrap();
        w.sync().unwrap();

        let current = reader.document(KIND, "main").unwrap().unwrap();
        assert_eq!((current.id, current.supersedes), (v2, Some(v1)));
        assert_eq!(reader.live_wal(w.source_id(), "s").unwrap().len(), 2);

        let gone = archive.delete_document(KIND, "main").unwrap();
        assert!(gone.is_some());
        assert_eq!(reader.document(KIND, "main").unwrap(), None);
        assert_eq!(archive.delete_document(KIND, "main").unwrap(), None);

        w.finalize((2_000, 0)).unwrap();
        archive.join().unwrap();
        assert_eq!(
            Archive::open(&path)
                .unwrap()
                .document_history(KIND, "main")
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn a_writer_reopening_an_archive_from_before_the_table_creates_it_on_first_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.dendro");
        previous_release_archive(&path);

        let mut archive = Writer::open(&path, Box::new(Never)).unwrap();
        // A reader opened before the table exists sees it once it does.
        let reader = Archive::open(&path).unwrap();
        assert_eq!(reader.document(KIND, "main").unwrap(), None);
        archive.put_document(KIND, "main", SCHEMA, "1").unwrap();
        assert_eq!(reader.document(KIND, "main").unwrap().unwrap().body, "1");
        archive.join().unwrap();
    }
}
