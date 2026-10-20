// The archive half; the writer tests below also need `write`.
#![cfg(feature = "archive")]

//! Archive-level documents (FORMAT.md §3.6): append-only revisions keyed by
//! `(kind, name)`, carried by this crate's copies, readable from archives
//! written before the table existed.

use std::collections::BTreeMap;
use std::path::Path;

use dendro::archive::{Archive, ArchiveMut, Revision, SourceMeta, WalRow};
use dendro::rewrite::{copy_documents_into, copy_sources_into, CopySpec};
use dendro::segment::{EncodeContext, EncodeResult, SegmentEncoder};
use dendro::Error;

struct Never;
impl SegmentEncoder for Never {
    fn encode(&self, _ctx: &EncodeContext<'_>, _rows: &[WalRow]) -> EncodeResult {
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

/// One revision as a copy must carry it: `(uuid, schema, body,
/// created_wall_ns)`, with `None` for a deletion's schema and body. The id
/// is left out because a copy renumbers it.
type Carried = (String, Option<String>, Option<String>, i64);

/// Every revision of `(kind, name)`, oldest first, as [`Carried`].
fn revisions(db: &Archive, kind: &str, name: &str) -> Vec<Carried> {
    db.document_history(kind, name)
        .unwrap()
        .into_iter()
        .map(|r| match r {
            Revision::Put(d) => (d.uuid, Some(d.schema), Some(d.body), d.created_wall_ns),
            Revision::Delete(d) => (d.uuid, None, None, d.created_wall_ns),
            _ => unreachable!("a revision this test does not know"),
        })
        .collect()
}

fn key(kind: &str, name: &str) -> (String, String) {
    (kind.to_string(), name.to_string())
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

/// A v4 archive, as releases up to 0.5.1 create it: this build's schema
/// without the `documents` and `segment_indexes` tables, with the index as
/// the last column of `segments`, stamped 4. Those are the differences
/// between the two schemas.
fn v4_archive(path: &Path) {
    let mut db = ArchiveMut::create(path).unwrap();
    db.insert_source(&source()).unwrap();
    drop(db);
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.execute_batch(
        "DROP TABLE documents; DROP TABLE segment_indexes; \
         ALTER TABLE segments ADD COLUMN caller_index BLOB; PRAGMA user_version = 4;",
    )
    .unwrap();
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
        (v1.id, None, r#"{"v":1}"#)
    );
    assert_eq!(first.uuid, v1.uuid, "the write returns the revision's uuid");
    assert_eq!((first.kind.as_str(), first.name.as_str()), (KIND, "main"));
    assert_eq!(first.schema, SCHEMA);

    let v2 = db.put_document(KIND, "main", SCHEMA, r#"{"v":2}"#).unwrap();
    let current = db.document(KIND, "main").unwrap().unwrap();
    assert_eq!(
        (current.id, current.supersedes, current.body.as_str()),
        (v2.id, Some(v1.id), r#"{"v":2}"#)
    );
    assert_eq!(current.uuid, v2.uuid);
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
        Revision::Delete(d) => {
            assert_eq!((d.id, d.supersedes), (gone.id, Some(v1.id)));
            assert_eq!(d.uuid, gone.uuid);
        }
        other => panic!("expected a deletion, got {other:?}"),
    }

    // Deleting a deleted document writes nothing.
    assert_eq!(db.delete_document(KIND, "main").unwrap(), None);
    assert_eq!(db.document_history(KIND, "main").unwrap().len(), 2);

    // A put after a delete continues the chain from the deletion.
    let v3 = db.put_document(KIND, "main", SCHEMA, "3").unwrap();
    let current = db.document(KIND, "main").unwrap().unwrap();
    assert_eq!((current.id, current.supersedes), (v3.id, Some(gone.id)));
}

#[test]
fn a_conditional_put_is_refused_when_another_revision_landed() {
    let mut db = ArchiveMut::create_in_memory().unwrap();
    // `None` means "there is no current document".
    db.put_document_if_current(KIND, "main", SCHEMA, "1", None)
        .unwrap();
    let v1 = db.document(KIND, "main").unwrap().unwrap().uuid;
    match db.put_document_if_current(KIND, "main", SCHEMA, "0", None) {
        Err(Error::DocumentConflict {
            expected, current, ..
        }) => assert_eq!((expected, current), (None, Some(v1.clone()))),
        other => panic!("expected a conflict, got {other:?}"),
    }

    // Two editors read v1; the first saves, the second is refused.
    db.put_document_if_current(KIND, "main", SCHEMA, "2", Some(&v1))
        .unwrap();
    let v2 = db.document(KIND, "main").unwrap().unwrap().uuid;
    match db.put_document_if_current(KIND, "main", SCHEMA, "99", Some(&v1)) {
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
    let v4 = db
        .put_document_if_current(KIND, "main", SCHEMA, "4", None)
        .unwrap();
    // Saves chain on the returned uuid, with no read in between.
    let v5 = db
        .put_document_if_current(KIND, "main", SCHEMA, "5", Some(&v4.uuid))
        .unwrap();
    db.put_document_if_current(KIND, "main", SCHEMA, "6", Some(&v5.uuid))
        .unwrap();
    assert_eq!(db.document(KIND, "main").unwrap().unwrap().body, "6");
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
    // A delete checks the key the same way.
    for (kind, name) in [("", "main"), (KIND, ""), ("dendro.view", "main")] {
        let got = db.delete_document(kind, name);
        assert!(
            matches!(got, Err(Error::Message(_))),
            "delete {kind:?}/{name:?}: {got:?}"
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
    src.put_document(KIND, "main", "app.view/2", "2").unwrap();
    src.delete_document(KIND, "main").unwrap();
    src.put_document(KIND, "main", "app.view/3", "4").unwrap();
    src.put_document("app.event", "e", "app.event/1", "{}")
        .unwrap();
    drop(src);
    let src = Archive::open(&src_path).unwrap();
    // The copy must keep the stamps, not take its own: let the clock move
    // past every one of them first (millisecond resolution).
    std::thread::sleep(std::time::Duration::from_millis(5));

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

    // Copying the same archive again adds nothing, and makes nothing current.
    let again = dst.transaction(|tx| copy_documents_into(&src, tx)).unwrap();
    assert_eq!(again.inserted, 0);
    assert!(again.current.is_empty());
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
    let (from_a, from_b) = combined
        .transaction(|tx| Ok((copy_documents_into(&a, tx)?, copy_documents_into(&b, tx)?)))
        .unwrap();
    // The first input brings base and its edit; the second only its edit,
    // which displaces the first input's without superseding it.
    assert_eq!(from_a.inserted, 2);
    assert_eq!(from_a.current, vec![key(KIND, "main")]);
    assert!(from_a.forked.is_empty());
    assert_eq!(from_b.inserted, 1);
    assert_eq!(from_b.current, vec![key(KIND, "main")]);
    assert_eq!(from_b.forked.len(), 1);
    let fork = &from_b.forked[0];
    assert_eq!((fork.kind.as_str(), fork.name.as_str()), (KIND, "main"));
    // Displaced: the first input's edit. Current: the second's.
    let history = revisions(&combined, KIND, "main");
    assert_eq!(fork.displaced, history[1].0);
    assert_eq!(fork.current, history[2].0);
    // The shared base once, then each edit; the input copied last is current.
    let history = revisions(&combined, KIND, "main");
    let bodies: Vec<Option<&str>> = history.iter().map(|r| r.2.as_deref()).collect();
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
fn a_new_archive_is_schema_5_with_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("new.dendro");
    drop(ArchiveMut::create(&path).unwrap());
    assert_eq!(
        dendro::archive::sniff(&path).unwrap(),
        dendro::archive::Sniff::Stamped { version: 5 }
    );
    assert_eq!(Archive::open(&path).unwrap().schema_version().unwrap(), 5);
    let in_memory = ArchiveMut::create_in_memory().unwrap();
    assert_eq!(in_memory.schema_version().unwrap(), 5);
    let reopened = Archive::open_bytes(in_memory.serialize().unwrap()).unwrap();
    assert_eq!(reopened.schema_version().unwrap(), 5);
}

#[test]
fn a_v4_archive_reads_as_holding_no_documents_and_refuses_them() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v4.dendro");
    v4_archive(&path);

    let reader = Archive::open(&path).unwrap();
    assert_eq!(reader.schema_version().unwrap(), 4);
    assert_eq!(reader.document(KIND, "main").unwrap(), None);
    assert!(reader.documents(KIND).unwrap().is_empty());
    assert!(reader.document_kinds().unwrap().is_empty());
    assert!(reader.document_history(KIND, "main").unwrap().is_empty());
    assert_eq!(reader.read_sources().unwrap().len(), 1);
    drop(reader);

    let mut db = ArchiveMut::open(&path).unwrap();
    // Nothing to delete, so nothing written.
    assert_eq!(db.delete_document(KIND, "main").unwrap(), None);
    match db.put_document(KIND, "main", SCHEMA, "1") {
        Err(Error::SchemaTooOld { found, needs }) => assert_eq!((found, needs), (4, 5)),
        other => panic!("expected SchemaTooOld, got {other:?}"),
    }
    // What v4 holds can still be written, and the file stays v4.
    let id = db.read_sources().unwrap()[0].id;
    db.patch_source_metadata(id, &BTreeMap::from([("k".to_string(), "v".to_string())]))
        .unwrap();
    drop(db);

    let reader = Archive::open(&path).unwrap();
    assert_eq!(reader.schema_version().unwrap(), 4);
    let tables: i64 = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'documents%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(tables, 0, "never migrated in place");
    assert!(reader
        .verify(dendro::archive::Depth::Full)
        .unwrap()
        .is_sound());
}

#[test]
fn a_v4_archive_copied_forward_is_v5_and_takes_documents() {
    let dir = tempfile::tempdir().unwrap();
    let old = dir.path().join("v4.dendro");
    v4_archive(&old);
    let src = Archive::open(&old).unwrap();

    let new = dir.path().join("v5.dendro");
    let mut dst = ArchiveMut::create(&new).unwrap();
    let copied = dst
        .transaction(|tx| {
            let docs = copy_documents_into(&src, tx)?;
            let sources = copy_sources_into(&src, tx, &CopySpec::everything(), &Never)?;
            Ok((docs, sources))
        })
        .unwrap();
    assert_eq!(copied.0.inserted, 0);
    assert_eq!(copied.1, 1);
    dst.put_document(KIND, "main", SCHEMA, "1").unwrap();
    drop(dst);

    let reader = Archive::open(&new).unwrap();
    assert_eq!(reader.schema_version().unwrap(), 5);
    assert_eq!(reader.read_sources().unwrap().len(), 1);
    assert_eq!(reader.document(KIND, "main").unwrap().unwrap().body, "1");
}

#[test]
fn a_copy_of_documents_into_a_v4_archive_is_refused_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let old = dir.path().join("v4.dendro");
    v4_archive(&old);

    let mut src = ArchiveMut::create_in_memory().unwrap();
    src.put_document(KIND, "main", SCHEMA, "1").unwrap();
    let with_docs = Archive::open_bytes(src.serialize().unwrap()).unwrap();
    let without =
        Archive::open_bytes(ArchiveMut::create_in_memory().unwrap().serialize().unwrap()).unwrap();

    let mut db = ArchiveMut::open(&old).unwrap();
    // A source with no documents copies into v4: nothing v5 is written.
    db.transaction(|tx| copy_sources_into(&without, tx, &CopySpec::everything(), &Never))
        .unwrap();
    let got = db.transaction(|tx| copy_documents_into(&with_docs, tx));
    assert!(
        matches!(got, Err(Error::SchemaTooOld { found: 4, needs: 5 })),
        "{got:?}"
    );
    // Through copy_sources_into too, and the whole transaction rolls back.
    let mut with_source = ArchiveMut::create_in_memory().unwrap();
    with_source.insert_source(&source()).unwrap();
    with_source.put_document(KIND, "main", SCHEMA, "1").unwrap();
    let with_source = Archive::open_bytes(with_source.serialize().unwrap()).unwrap();
    let got =
        db.transaction(|tx| copy_sources_into(&with_source, tx, &CopySpec::everything(), &Never));
    assert!(matches!(got, Err(Error::SchemaTooOld { .. })), "{got:?}");
    assert_eq!(db.read_sources().unwrap().len(), 1, "only the v4 source");
    assert_eq!(db.schema_version().unwrap(), 4);
}

#[test]
fn the_wall_clock_is_stamped_at_millisecond_resolution() {
    let mut db = ArchiveMut::create_in_memory().unwrap();
    // A seconds-resolution clock stamps a whole number of seconds every
    // time; a millisecond one does so one time in a thousand. Twenty puts a
    // few milliseconds apart tell the two apart.
    let mut stamps = Vec::new();
    for i in 0..20 {
        db.put_document(KIND, "main", SCHEMA, &i.to_string())
            .unwrap();
        stamps.push(db.document(KIND, "main").unwrap().unwrap().created_wall_ns);
        std::thread::sleep(std::time::Duration::from_millis(3));
    }
    assert!(
        stamps.iter().all(|t| t % 1_000_000 == 0),
        "whole milliseconds: {stamps:?}"
    );
    assert!(
        stamps.iter().any(|t| t % 1_000_000_000 != 0),
        "finer than seconds: {stamps:?}"
    );
    assert!(stamps.windows(2).all(|w| w[0] <= w[1]));
}

#[test]
fn any_rfc_8259_body_is_accepted_and_stored_exactly() {
    let mut db = ArchiveMut::create_in_memory().unwrap();
    let deep = format!("{}{}", "[".repeat(5_000), "]".repeat(5_000));
    let bodies = [
        // JSON.stringify("\ud800") in JavaScript: a lone surrogate, escaped.
        r#""\ud800""#.to_string(),
        r#"{"a":"\udc00 😀"}"#.to_string(),
        // Deeper than serde_json's default limit of 128 and SQLite's 1000.
        deep,
        " 1.5e-3 ".to_string(),
    ];
    for (i, body) in bodies.iter().enumerate() {
        let name = format!("d{i}");
        db.put_document(KIND, &name, SCHEMA, body).unwrap();
        assert_eq!(&db.document(KIND, &name).unwrap().unwrap().body, body);
    }
}

#[test]
fn a_conditional_put_names_the_revision_by_uuid_so_it_survives_a_copy() {
    let mut src = ArchiveMut::create_in_memory().unwrap();
    src.put_document(KIND, "main", SCHEMA, "1").unwrap();
    let read = src.document(KIND, "main").unwrap().unwrap();
    let src = Archive::open_bytes(src.serialize().unwrap()).unwrap();

    // The destination holds other revisions first, so the copy's id for
    // the revision the editor read is not the id it read.
    let mut dst = ArchiveMut::create_in_memory().unwrap();
    dst.put_document("app.other", "x", "app.other/1", "{}")
        .unwrap();
    dst.transaction(|tx| copy_documents_into(&src, tx)).unwrap();
    let copied = dst.document(KIND, "main").unwrap().unwrap();
    assert_eq!(copied.uuid, read.uuid);
    assert_ne!(copied.id, read.id);
    dst.put_document_if_current(KIND, "main", SCHEMA, "2", Some(&read.uuid))
        .unwrap();
    assert_eq!(dst.document(KIND, "main").unwrap().unwrap().body, "2");
}

#[test]
fn a_copy_that_updates_a_document_it_supersedes_is_not_a_fork() {
    let mut a = ArchiveMut::create_in_memory().unwrap();
    a.put_document(KIND, "main", SCHEMA, "1").unwrap();
    let old = Archive::open_bytes(a.serialize().unwrap()).unwrap();
    a.put_document(KIND, "main", SCHEMA, "2").unwrap();
    a.put_document(KIND, "main", SCHEMA, "3").unwrap();
    let new = Archive::open_bytes(a.serialize().unwrap()).unwrap();

    let mut dst = ArchiveMut::create_in_memory().unwrap();
    let first = dst.transaction(|tx| copy_documents_into(&old, tx)).unwrap();
    assert_eq!(first.current, vec![key(KIND, "main")]);
    let second = dst.transaction(|tx| copy_documents_into(&new, tx)).unwrap();
    assert_eq!(second.inserted, 2);
    assert_eq!(second.current, vec![key(KIND, "main")]);
    assert!(second.forked.is_empty(), "3 supersedes 1 through 2");
    // The older input copied after the newer one changes nothing.
    let third = dst.transaction(|tx| copy_documents_into(&old, tx)).unwrap();
    assert_eq!(third, Default::default());
    assert_eq!(dst.document(KIND, "main").unwrap().unwrap().body, "3");

    // A document the destination deleted is not current, so an unrelated
    // revision brought back under its name displaces nothing.
    dst.delete_document(KIND, "main").unwrap();
    let mut other = ArchiveMut::create_in_memory().unwrap();
    other.put_document(KIND, "main", SCHEMA, "9").unwrap();
    let other = Archive::open_bytes(other.serialize().unwrap()).unwrap();
    let fourth = dst
        .transaction(|tx| copy_documents_into(&other, tx))
        .unwrap();
    assert_eq!(fourth.current, vec![key(KIND, "main")]);
    assert!(fourth.forked.is_empty());
}

/// Copies of one base: `P` deletes the document, `Q` edits it. Combined in
/// either order, the second input displaces the first: a delete against an
/// edit is a fork whichever lands last.
#[test]
fn a_delete_and_an_edit_of_one_base_fork_in_either_order() {
    let mut base = ArchiveMut::create_in_memory().unwrap();
    base.put_document(KIND, "main", SCHEMA, r#""base""#)
        .unwrap();
    let base = base.serialize().unwrap();
    let branch = |change: &dyn Fn(&mut ArchiveMut)| {
        let mut copy = ArchiveMut::create_in_memory().unwrap();
        let from = Archive::open_bytes(base.clone()).unwrap();
        copy.transaction(|tx| copy_documents_into(&from, tx))
            .unwrap();
        change(&mut copy);
        copy.serialize().unwrap()
    };
    let p = branch(&|db| {
        db.delete_document(KIND, "main").unwrap();
    });
    let q = branch(&|db| {
        db.put_document(KIND, "main", SCHEMA, r#""edit""#).unwrap();
    });

    for (first, second, body_after) in [(&p, &q, Some(r#""edit""#)), (&q, &p, None)] {
        let mut dst = ArchiveMut::create_in_memory().unwrap();
        let (a, b) = dst
            .transaction(|tx| {
                let a = copy_documents_into(&Archive::open_bytes(first.clone()).unwrap(), tx)?;
                let b = copy_documents_into(&Archive::open_bytes(second.clone()).unwrap(), tx)?;
                Ok((a, b))
            })
            .unwrap();
        assert!(a.forked.is_empty());
        assert_eq!(b.forked.len(), 1, "{body_after:?}: {b:?}");
        let history = revisions(&dst, KIND, "main");
        assert_eq!(history.len(), 3);
        assert_eq!(b.forked[0].displaced, history[1].0);
        assert_eq!(b.forked[0].current, history[2].0);
        assert_eq!(
            dst.document(KIND, "main").unwrap().map(|d| d.body),
            body_after.map(str::to_string)
        );
    }
}

/// The destination holds A then B; the input holds A, C, D (D superseding
/// C superseding A). Inserting C then D displaces B: the baseline is what
/// the destination held, not the copy's own earlier insert.
#[test]
fn a_fork_is_measured_against_the_destination_not_the_copys_own_inserts() {
    let mut base = ArchiveMut::create_in_memory().unwrap();
    base.put_document(KIND, "main", SCHEMA, r#""A""#).unwrap();
    let base = base.serialize().unwrap();
    let from_base = || {
        let mut copy = ArchiveMut::create_in_memory().unwrap();
        let from = Archive::open_bytes(base.clone()).unwrap();
        copy.transaction(|tx| copy_documents_into(&from, tx))
            .unwrap();
        copy
    };
    let mut dst = from_base();
    dst.put_document(KIND, "main", SCHEMA, r#""B""#).unwrap();
    let b = dst.document(KIND, "main").unwrap().unwrap().uuid;
    let mut src = from_base();
    src.put_document(KIND, "main", SCHEMA, r#""C""#).unwrap();
    src.put_document(KIND, "main", SCHEMA, r#""D""#).unwrap();
    let d = src.document(KIND, "main").unwrap().unwrap().uuid;
    let src = Archive::open_bytes(src.serialize().unwrap()).unwrap();

    let copied = dst.transaction(|tx| copy_documents_into(&src, tx)).unwrap();
    assert_eq!(copied.inserted, 2);
    assert_eq!(copied.forked.len(), 1);
    assert_eq!(
        (&copied.forked[0].displaced, &copied.forked[0].current),
        (&b, &d)
    );
}

#[test]
fn a_revision_held_with_different_contents_refuses_the_copy() {
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.dendro");
    let mut src = ArchiveMut::create(&src_path).unwrap();
    src.put_document(KIND, "main", SCHEMA, "1").unwrap();
    drop(src);

    // Each field the comparison covers, altered in turn in a copy.
    for column in ["kind", "name", "schema", "body"] {
        let dst_path = dir.path().join(format!("dst-{column}.dendro"));
        let src = Archive::open(&src_path).unwrap();
        let mut dst = ArchiveMut::create(&dst_path).unwrap();
        dst.transaction(|tx| copy_documents_into(&src, tx)).unwrap();
        drop(dst);
        let raw = rusqlite::Connection::open(&dst_path).unwrap();
        raw.execute(&format!("UPDATE documents SET {column} = '2'"), [])
            .unwrap();
        drop(raw);

        let mut dst = ArchiveMut::open(&dst_path).unwrap();
        let got = dst.transaction(|tx| copy_documents_into(&src, tx));
        assert!(
            matches!(&got, Err(Error::Message(m)) if m.contains("different contents")),
            "{column}: {got:?}"
        );
    }
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
        let read = reader.document(KIND, "main").unwrap().unwrap();
        assert_eq!((read.id, &read.uuid), (v1.id, &v1.uuid));

        // A conflict is the caller's answer, and the writer keeps going.
        match archive.put_document_if_current(KIND, "main", SCHEMA, "{}", None) {
            Err(Error::DocumentConflict { current, .. }) => {
                assert_eq!(current.as_deref(), Some(read.uuid.as_str()))
            }
            other => panic!("expected a conflict, got {other:?}"),
        }
        assert!(matches!(
            archive.put_document("dendro.x", "main", SCHEMA, "{}"),
            Err(Error::Message(_))
        ));
        let v2 = archive
            .put_document_if_current(KIND, "main", SCHEMA, r#"{"v":2}"#, Some(&read.uuid))
            .unwrap();
        w.wal(vec![row(2_000)]).unwrap();
        w.sync().unwrap();

        let current = reader.document(KIND, "main").unwrap().unwrap();
        assert_eq!((current.id, current.supersedes), (v2.id, Some(v1.id)));
        assert_eq!(current.uuid, v2.uuid);
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
    fn a_writer_resuming_a_v4_archive_records_and_refuses_documents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v4.dendro");
        v4_archive(&path);
        let id = Archive::open(&path).unwrap().read_sources().unwrap()[0].id;

        let mut archive = Writer::open(&path, Box::new(Never)).unwrap();
        let (mut w, _) = archive.resume_source(id, 1_000).unwrap();
        match archive.put_document(KIND, "main", SCHEMA, "1") {
            Err(Error::SchemaTooOld { found, needs }) => assert_eq!((found, needs), (4, 5)),
            other => panic!("expected SchemaTooOld, got {other:?}"),
        }
        // The recording is unaffected by the refusal.
        w.wal(vec![row(2_000)]).unwrap();
        w.finalize((2_000, 0)).unwrap();
        archive.join().unwrap();

        let reader = Archive::open(&path).unwrap();
        assert_eq!(reader.schema_version().unwrap(), 4);
        assert_eq!(reader.live_wal(id, "s").unwrap().len(), 1);
        assert_eq!(reader.document(KIND, "main").unwrap(), None);
        // Opening a v4 archive to write gives it no v5 structure.
        let tables: i64 = rusqlite::Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'documents%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(tables, 0);
    }
}
