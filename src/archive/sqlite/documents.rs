//! Archive-level documents: the `documents` table (FORMAT.md §3.6).
//!
//! Everything else in an archive belongs to a source. A document belongs to
//! the archive: a saved view, a report's selection, an event that spans
//! every source. Revisions are appended and never changed, so a document
//! keeps its history, and the copies this crate makes carry every revision
//! with its identity. Replication does not (FORMAT.md §3.6).

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, OptionalExtension};

use super::{Archive, ArchiveMut, Transaction};
use crate::error::{Error, Result};

/// Kinds that start with this are dendro's, and a caller's
/// [`put_document`](ArchiveMut::put_document) refuses them. dendro writes
/// none today; the prefix is held so it can later without colliding with a
/// caller's namespace.
pub const RESERVED_KIND_PREFIX: &str = "dendro.";

/// The table and its index, created with every new archive (schema v5). A
/// v4 archive has neither and is never given them (FORMAT.md §8).
pub(super) const DOCUMENTS_SQL: &str = "
CREATE TABLE documents(
  id INTEGER PRIMARY KEY,
  -- The revision's identity across files: minted at insert and carried by
  -- this crate's copies, so a copy that meets a revision it already holds
  -- skips it.
  uuid TEXT NOT NULL UNIQUE,
  kind TEXT NOT NULL,
  name TEXT NOT NULL,
  -- The caller's name and version for the body's type. Stored, never read.
  -- NULL, with body, on a deletion.
  schema TEXT,
  -- JSON, checked to parse at insert and otherwise never read.
  body TEXT,
  created_wall_ns INTEGER NOT NULL,
  -- The revision that was current for (kind, name) when this one was
  -- written. Provenance only: the current revision is the newest by id.
  supersedes INTEGER REFERENCES documents(id),
  CHECK ((schema IS NULL) = (body IS NULL))
);
CREATE INDEX documents_by_name ON documents(kind, name, id);
";

/// The current revision of one document: its newest, when that revision is
/// not a deletion. See [`Archive::document`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Document {
    /// The revision's row id in this file. A copy renumbers it, so it is
    /// not an identity.
    pub id: i64,
    /// The revision's identity, carried by the copies this crate makes, and
    /// what [`put_document_if_current`](ArchiveMut::put_document_if_current)
    /// compares against. Replication does not carry documents
    /// (FORMAT.md §3.6).
    pub uuid: String,
    /// The caller's namespace for documents of one purpose.
    pub kind: String,
    /// Which document of that kind.
    pub name: String,
    /// The body's type and version, as the writer named it.
    pub schema: String,
    /// The document itself: JSON text, as written.
    pub body: String,
    /// The wall clock when the revision was written, in nanoseconds since
    /// the epoch, at millisecond resolution. Provenance only.
    pub created_wall_ns: i64,
    /// The revision that was current when this one was written, by this
    /// file's id. `None` for a document's first revision.
    pub supersedes: Option<i64>,
}

/// A revision that deleted a document. See [`Revision`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Deletion {
    /// The revision's row id in this file.
    pub id: i64,
    /// The revision's identity, carried by the copies this crate makes.
    pub uuid: String,
    /// The deleted document's kind.
    pub kind: String,
    /// The deleted document's name.
    pub name: String,
    /// The wall clock when the deletion was written, as in
    /// [`Document::created_wall_ns`].
    pub created_wall_ns: i64,
    /// The revision it deleted, by this file's id.
    pub supersedes: Option<i64>,
}

/// One entry in a document's history: a body written, or the document
/// deleted. See [`Archive::document_history`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Revision {
    /// A body was written.
    Put(Document),
    /// The document was deleted.
    Delete(Deletion),
}

impl Revision {
    /// The revision's row id in this file.
    pub fn id(&self) -> i64 {
        match self {
            Revision::Put(d) => d.id,
            Revision::Delete(d) => d.id,
        }
    }

    /// The revision that was current when this one was written.
    ///
    /// Two revisions with the same `supersedes` are a fork: two writers each
    /// replaced the revision they had read. The newer by id is current; the
    /// other stays in the history.
    pub fn supersedes(&self) -> Option<i64> {
        match self {
            Revision::Put(d) => d.supersedes,
            Revision::Delete(d) => d.supersedes,
        }
    }
}

/// The revision a write created: its id in this file and its uuid, which
/// is what [`put_document_if_current`](ArchiveMut::put_document_if_current)
/// takes, so a caller can chain conditional saves without reading the
/// document back between them.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RevisionId {
    /// The revision's row id in this file.
    pub id: i64,
    /// The revision's identity, carried by the copies this crate makes.
    pub uuid: String,
}

/// A document whose current revision a copy replaced with one that does
/// not supersede it. See [`CopiedDocuments::forked`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Fork {
    /// The document's kind.
    pub kind: String,
    /// The document's name.
    pub name: String,
    /// The uuid of the revision that was current in the destination before
    /// the copy, and is now only in the history. It can be a deletion.
    pub displaced: String,
    /// The uuid of the revision that is current now, which the copy
    /// inserted. It can be a deletion.
    pub current: String,
}

const COLUMNS: &str = "id, uuid, kind, name, schema, body, created_wall_ns, supersedes";

fn revision_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Revision> {
    let id: i64 = row.get(0)?;
    let uuid: String = row.get(1)?;
    let kind: String = row.get(2)?;
    let name: String = row.get(3)?;
    let schema: Option<String> = row.get(4)?;
    let body: Option<String> = row.get(5)?;
    let created_wall_ns: i64 = row.get(6)?;
    let supersedes: Option<i64> = row.get(7)?;
    // The table's CHECK keeps the two NULL together, so a mixed pair is a
    // file this crate did not write; it reads as a deletion rather than a
    // body with no type.
    Ok(match (schema, body) {
        (Some(schema), Some(body)) => Revision::Put(Document {
            id,
            uuid,
            kind,
            name,
            schema,
            body,
            created_wall_ns,
            supersedes,
        }),
        _ => Revision::Delete(Deletion {
            id,
            uuid,
            kind,
            name,
            created_wall_ns,
            supersedes,
        }),
    })
}

/// Whether this file has the `documents` table. An archive written before
/// it has none, and every read answers "no documents" for it.
fn has_documents(conn: &Connection) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'documents')",
        [],
        |row| row.get::<_, bool>(0),
    )
    .map_err(Error::sqlite("failed to look for the documents table"))
}

/// Refuse a document write into an archive whose schema has no
/// `documents` table. Decided by the header, as the open gate is.
fn check_writable(conn: &Connection) -> Result<()> {
    let found: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(Error::sqlite("failed to read the schema version"))?;
    if found < super::DOCUMENTS_SCHEMA_VERSION {
        return Err(Error::SchemaTooOld {
            found,
            needs: super::DOCUMENTS_SCHEMA_VERSION,
        });
    }
    Ok(())
}

/// The newest revision of `(kind, name)`, deletion or not, whole.
fn newest(conn: &Connection, kind: &str, name: &str) -> Result<Option<Revision>> {
    conn.query_row(
        &format!(
            "SELECT {COLUMNS} FROM documents WHERE kind = ?1 AND name = ?2 \
             ORDER BY id DESC LIMIT 1"
        ),
        rusqlite::params![kind, name],
        revision_from_row,
    )
    .optional()
    .map_err(Error::sqlite(format!(
        "failed to read document {kind}/{name}"
    )))
}

/// The newest revision of `(kind, name)` without its body: what a write
/// needs to supersede it, at the cost of a row lookup rather than a body.
struct Head {
    id: i64,
    uuid: String,
    /// Whether it is a deletion.
    deleted: bool,
}

fn head(conn: &Connection, kind: &str, name: &str) -> Result<Option<Head>> {
    conn.query_row(
        "SELECT id, uuid, body IS NULL FROM documents WHERE kind = ?1 AND name = ?2 \
         ORDER BY id DESC LIMIT 1",
        rusqlite::params![kind, name],
        |row| {
            Ok(Head {
                id: row.get(0)?,
                uuid: row.get(1)?,
                deleted: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(Error::sqlite(format!(
        "failed to read document {kind}/{name}"
    )))
}

/// The wall clock in nanoseconds, from SQLite rather than `std`:
/// `SystemTime::now` panics on `wasm32-unknown-unknown`, where an archive is
/// assembled in memory, and SQLite's clock comes from its VFS there.
/// Millisecond resolution, which is what `'subsec'` gives.
fn wall_now_ns(conn: &Connection) -> Result<i64> {
    conn.query_row(
        "SELECT CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000000",
        [],
        |row| row.get(0),
    )
    .map_err(Error::sqlite("failed to read the clock"))
}

fn check_key(kind: &str, name: &str) -> Result<()> {
    if kind.is_empty() || name.is_empty() {
        return Err(Error::Message(format!(
            "a document needs a kind and a name; got kind {kind:?}, name {name:?}"
        )));
    }
    if kind.starts_with(RESERVED_KIND_PREFIX) {
        return Err(Error::Message(format!(
            "document kind {kind:?} is reserved: kinds starting {RESERVED_KIND_PREFIX:?} are \
             dendro's"
        )));
    }
    Ok(())
}

/// Whether `body` is JSON (RFC 8259), without building it.
///
/// `IgnoredAny` walks the text and keeps nothing, so the check costs no
/// memory beyond the text itself (a `serde_json::Value` of a 56 MB body
/// peaked at several times its size). It also has no nesting limit and
/// accepts an escaped lone surrogate (`"\ud800"`), both of which RFC 8259's
/// grammar allows and JavaScript's `JSON.stringify` can produce; a parse into
/// `Value` refuses both, and SQLite's `json_valid` refuses nesting past 1000.
fn check_json(kind: &str, name: &str, body: &str) -> Result<()> {
    serde_json::from_str::<serde_core::de::IgnoredAny>(body)
        .map(|_| ())
        .map_err(|e| Error::Message(format!("document {kind}/{name}: the body is not JSON: {e}")))
}

/// One revision's columns, borrowed. `content` is `(schema, body)`, `None`
/// for a deletion.
struct NewRevision<'a> {
    uuid: &'a str,
    kind: &'a str,
    name: &'a str,
    content: Option<(&'a str, &'a str)>,
    created_wall_ns: i64,
    supersedes: Option<i64>,
}

fn insert_revision(conn: &Connection, rev: &NewRevision<'_>) -> Result<i64> {
    let NewRevision {
        uuid,
        kind,
        name,
        content,
        created_wall_ns,
        supersedes,
    } = *rev;
    conn.execute(
        "INSERT INTO documents(uuid, kind, name, schema, body, created_wall_ns, supersedes) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            uuid,
            kind,
            name,
            content.map(|c| c.0),
            content.map(|c| c.1),
            created_wall_ns,
            supersedes
        ],
    )
    .map_err(Error::sqlite(format!(
        "failed to write document {kind}/{name}"
    )))?;
    Ok(conn.last_insert_rowid())
}

/// `expected`: `None` writes unconditionally; `Some(e)` refuses unless the
/// current document's uuid is `e` (`Some(None)`: unless there is none).
fn put(
    conn: &Connection,
    kind: &str,
    name: &str,
    schema: &str,
    body: &str,
    expected: Option<Option<&str>>,
) -> Result<RevisionId> {
    check_key(kind, name)?;
    if schema.is_empty() {
        return Err(Error::Message(format!(
            "document {kind}/{name} needs a schema naming its body's type"
        )));
    }
    check_json(kind, name, body)?;
    check_writable(conn)?;
    let head = head(conn, kind, name)?;
    if let Some(expected) = expected {
        let current = head
            .as_ref()
            .filter(|h| !h.deleted)
            .map(|h| h.uuid.as_str());
        if current != expected {
            return Err(Error::DocumentConflict {
                kind: kind.to_string(),
                name: name.to_string(),
                expected: expected.map(str::to_string),
                current: current.map(str::to_string),
            });
        }
    }
    let uuid = super::mint_uuid(conn)?;
    let created_wall_ns = wall_now_ns(conn)?;
    let id = insert_revision(
        conn,
        &NewRevision {
            uuid: &uuid,
            kind,
            name,
            content: Some((schema, body)),
            created_wall_ns,
            supersedes: head.map(|h| h.id),
        },
    )?;
    Ok(RevisionId { id, uuid })
}

fn delete(conn: &Connection, kind: &str, name: &str) -> Result<Option<RevisionId>> {
    check_key(kind, name)?;
    if !has_documents(conn)? {
        return Ok(None);
    }
    let Some(current) = head(conn, kind, name)?.filter(|h| !h.deleted) else {
        return Ok(None);
    };
    let uuid = super::mint_uuid(conn)?;
    let created_wall_ns = wall_now_ns(conn)?;
    let id = insert_revision(
        conn,
        &NewRevision {
            uuid: &uuid,
            kind,
            name,
            content: None,
            created_wall_ns,
            supersedes: Some(current.id),
        },
    )?;
    Ok(Some(RevisionId { id, uuid }))
}

/// What [`copy_documents_into`](crate::rewrite::copy_documents_into) did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CopiedDocuments {
    /// Revisions inserted. A revision the destination already held is not
    /// counted.
    pub inserted: usize,
    /// The `(kind, name)` of every document this copy inserted a revision
    /// of, in `(kind, name)` order. Inserted revisions take the newest ids,
    /// so each of these documents' newest revision, deletion included, is
    /// the newest one this copy inserted. That is the input's own current
    /// revision unless the destination already held that one; then an
    /// older revision the input brought is current, and the document is in
    /// `forked`. A document whose revisions the destination already held is
    /// not listed.
    pub current: Vec<(String, String)>,
    /// The documents in `current` whose new current revision does not
    /// supersede, directly or through earlier revisions, the revision the
    /// destination had as newest before the copy. That revision is now only
    /// in the history: two inputs held two versions of one document, and the
    /// later one won. A deletion counts on either side, so a delete in one
    /// input and an edit in another are a fork in either order. A
    /// destination whose newest revision was a deletion with no revision in
    /// common with the incoming one is not forked: the document was gone,
    /// and an unrelated one took its name. In `(kind, name)` order. A caller
    /// combining archives checks this to choose between the two or rename
    /// one.
    pub forked: Vec<Fork>,
}

/// Every revision `src` holds, oldest first, into `dst`, skipping any whose
/// uuid `dst` already has, which must then match it. See
/// [`copy_documents_into`](crate::rewrite::copy_documents_into).
///
/// Read with one statement, row by row, so a copy holds one body at a time
/// and sees one snapshot of `src` without a transaction of its own.
fn copy_documents(src: &Archive, dst: &Connection) -> Result<CopiedDocuments> {
    let mut report = CopiedDocuments::default();
    if !has_documents(&src.conn)? {
        return Ok(report);
    }
    let mut stmt = src
        .conn
        .prepare(&format!("SELECT {COLUMNS} FROM documents ORDER BY id"))
        .map_err(Error::sqlite("failed to query documents"))?;
    let mut rows = stmt
        .query([])
        .map_err(Error::sqlite("failed to query documents"))?;
    let read = |e| Error::sqlite("failed to read a document revision")(e);
    let mut ensured = false;
    // Source id -> destination id, for renumbering `supersedes`.
    let mut ids: BTreeMap<i64, i64> = BTreeMap::new();
    // Per document this copy inserted into: the destination's current
    // revision before the first insert (`None` if it had none), and the id
    // of the last revision inserted.
    let mut touched: BTreeMap<(String, String), (Option<Head>, i64)> = BTreeMap::new();
    // The lowest id this copy can assign: everything below it the
    // destination held before. Set when the first revision is inserted.
    let mut first_inserted = i64::MAX;
    while let Some(row) = rows
        .next()
        .map_err(Error::sqlite("failed to read a document revision"))?
    {
        if !ensured {
            check_writable(dst)?;
            ensured = true;
        }
        let src_id: i64 = row.get(0).map_err(read)?;
        let uuid = row.get_ref(1).and_then(|v| Ok(v.as_str()?)).map_err(read)?;
        let kind = row.get_ref(2).and_then(|v| Ok(v.as_str()?)).map_err(read)?;
        let name = row.get_ref(3).and_then(|v| Ok(v.as_str()?)).map_err(read)?;
        let schema = row
            .get_ref(4)
            .and_then(|v| Ok(v.as_str_or_null()?))
            .map_err(read)?;
        let body = row
            .get_ref(5)
            .and_then(|v| Ok(v.as_str_or_null()?))
            .map_err(read)?;
        let created_wall_ns: i64 = row.get(6).map_err(read)?;
        let src_supersedes: Option<i64> = row.get(7).map_err(read)?;
        let content = match (schema, body) {
            (Some(s), Some(b)) => Some((s, b)),
            _ => None,
        };

        // Compared in SQL, so the held body is not read out to compare it.
        let held: Option<(i64, bool)> = dst
            .query_row(
                "SELECT id, kind IS ?2 AND name IS ?3 AND schema IS ?4 AND body IS ?5 \
                 FROM documents WHERE uuid = ?1",
                rusqlite::params![uuid, kind, name, content.map(|c| c.0), content.map(|c| c.1)],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(Error::sqlite("failed to look up a document revision"))?;
        if let Some((id, same)) = held {
            if !same {
                return Err(Error::Message(format!(
                    "document revision {uuid} ({kind}/{name}) is in both archives with \
                     different contents; a revision never changes, so one of the two files is \
                     wrong, and the copy cannot tell which"
                )));
            }
            ids.insert(src_id, id);
            continue;
        }
        // A pointer the source cannot resolve (a file this crate did not
        // write; ours are written superseding a row that exists) is carried
        // as unknown rather than failing the copy.
        let supersedes = match src_supersedes {
            None => None,
            Some(s) => match ids.get(&s) {
                Some(&mapped) => Some(mapped),
                None => {
                    tracing::warn!(
                        "document {kind}/{name}: revision {src_id} supersedes {s}, which the \
                         source archive does not hold; copied as superseding nothing"
                    );
                    None
                }
            },
        };
        let key = (kind.to_string(), name.to_string());
        // The destination's newest revision before this copy touched the
        // document, captured once: later inserts of this copy must not
        // become the baseline.
        let before = match touched.remove(&key) {
            Some((before, _)) => before,
            None => head(dst, kind, name)?,
        };
        let id = insert_revision(
            dst,
            &NewRevision {
                uuid,
                kind,
                name,
                content,
                created_wall_ns,
                supersedes,
            },
        )?;
        first_inserted = first_inserted.min(id);
        ids.insert(src_id, id);
        touched.insert(key, (before, id));
        report.inserted += 1;
    }
    // Every insert takes the newest id, so the last revision this copy
    // inserted for a document is that document's newest.
    for ((kind, name), (before, last)) in touched {
        if let Some(before) = before {
            let (reaches, shares) = ancestry(dst, last, before.id, first_inserted)?;
            // A deletion with nothing in common with the incoming chain was
            // a document that no longer existed; anything else the new
            // revision does not supersede was displaced.
            if !reaches && (!before.deleted || shares) {
                let current = head(dst, &kind, &name)?.map(|h| h.uuid).unwrap_or_default();
                report.forked.push(Fork {
                    kind: kind.clone(),
                    name: name.clone(),
                    displaced: before.uuid,
                    current,
                });
            }
        }
        report.current.push((kind, name));
    }
    Ok(report)
}

/// Follow `supersedes` back from revision `id`: whether the chain reaches
/// `target`, and whether it passes through any revision below `first_new`,
/// one the destination held before the copy.
fn ancestry(conn: &Connection, id: i64, target: i64, first_new: i64) -> Result<(bool, bool)> {
    let mut at = Some(id);
    let mut seen = BTreeSet::new();
    let mut shares = false;
    while let Some(cur) = at {
        if cur == target {
            return Ok((true, true));
        }
        shares |= cur < first_new;
        // A cycle cannot be written by this crate; stop rather than spin.
        if !seen.insert(cur) {
            break;
        }
        at = conn
            .query_row(
                "SELECT supersedes FROM documents WHERE id = ?1",
                [cur],
                |row| row.get(0),
            )
            .optional()
            .map_err(Error::sqlite("failed to follow a document's history"))?
            .flatten();
    }
    Ok((false, shares))
}

impl Archive {
    /// The current document `(kind, name)`: its newest revision, or `None`
    /// when it has none or the newest is a deletion.
    ///
    /// "Newest" is by id, which is insertion order in this file. An archive
    /// from before the `documents` table reads as holding none.
    pub fn document(&self, kind: &str, name: &str) -> Result<Option<Document>> {
        if !has_documents(&self.conn)? {
            return Ok(None);
        }
        Ok(match newest(&self.conn, kind, name)? {
            Some(Revision::Put(d)) => Some(d),
            _ => None,
        })
    }

    /// Every current document of `kind`, by name. Deleted documents are
    /// left out; [`document_history`](Self::document_history) has them.
    pub fn documents(&self, kind: &str) -> Result<Vec<Document>> {
        if !has_documents(&self.conn)? {
            return Ok(Vec::new());
        }
        let mut stmt = self
            .conn
            .prepare(&format!(
                "SELECT {COLUMNS} FROM documents d WHERE kind = ?1 \
                 AND id = (SELECT MAX(id) FROM documents WHERE kind = d.kind AND name = d.name) \
                 ORDER BY name"
            ))
            .map_err(Error::sqlite(format!("failed to query {kind} documents")))?;
        let rows = stmt
            .query_map([kind], revision_from_row)
            .map_err(Error::sqlite(format!("failed to query {kind} documents")))?;
        // A document whose newest revision is a deletion is not current.
        let mut out = Vec::new();
        for row in rows {
            if let Revision::Put(d) =
                row.map_err(Error::sqlite(format!("failed to read a {kind} document")))?
            {
                out.push(d);
            }
        }
        Ok(out)
    }

    /// Every kind with at least one current document, alphabetically.
    pub fn document_kinds(&self) -> Result<Vec<String>> {
        if !has_documents(&self.conn)? {
            return Ok(Vec::new());
        }
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT kind FROM documents d WHERE body IS NOT NULL \
                 AND id = (SELECT MAX(id) FROM documents WHERE kind = d.kind AND name = d.name) \
                 ORDER BY kind",
            )
            .map_err(Error::sqlite("failed to query document kinds"))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(Error::sqlite("failed to query document kinds"))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::sqlite("failed to read a document kind"))
    }

    /// Every revision of `(kind, name)`, oldest first, deletions included.
    /// The last is current unless it is a [`Revision::Delete`].
    pub fn document_history(&self, kind: &str, name: &str) -> Result<Vec<Revision>> {
        if !has_documents(&self.conn)? {
            return Ok(Vec::new());
        }
        let mut stmt = self
            .conn
            .prepare(&format!(
                "SELECT {COLUMNS} FROM documents WHERE kind = ?1 AND name = ?2 ORDER BY id"
            ))
            .map_err(Error::sqlite(format!(
                "failed to query document {kind}/{name}"
            )))?;
        let rows = stmt
            .query_map(rusqlite::params![kind, name], revision_from_row)
            .map_err(Error::sqlite(format!(
                "failed to query document {kind}/{name}"
            )))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::sqlite(format!(
                "failed to read document {kind}/{name}"
            )))
    }
}

impl ArchiveMut {
    /// Write a new revision of document `(kind, name)` and return its id and
    /// uuid.
    ///
    /// The revision supersedes whatever was current, and becomes current
    /// itself; the earlier revisions stay in
    /// [`document_history`](Archive::document_history). Nothing is
    /// overwritten. `schema` names the body's type and version (say
    /// `"myapp.view/1"`) and is stored without being read. `body` must be
    /// JSON (RFC 8259, with no nesting limit), and is refused otherwise; it
    /// is stored as given.
    ///
    /// `kind` and `name` must be non-empty, and a `kind` starting with
    /// [`RESERVED_KIND_PREFIX`] is refused. A v4 archive, created by a
    /// release before documents, is refused as [`Error::SchemaTooOld`]: it
    /// is never migrated in place, and a copy of it made through
    /// [`crate::rewrite`] is a v5 archive that takes documents.
    ///
    /// For an archive a [`Writer`](crate::writer::Writer) is recording, which
    /// an `ArchiveMut` cannot open, use
    /// [`Writer::put_document`](crate::writer::Writer::put_document).
    pub fn put_document(
        &mut self,
        kind: &str,
        name: &str,
        schema: &str,
        body: &str,
    ) -> Result<RevisionId> {
        self.transaction(|tx| tx.put_document(kind, name, schema, body))
    }

    /// [`put_document`](Self::put_document), refused with
    /// [`Error::DocumentConflict`] unless the current document is the
    /// revision whose [`uuid`](Document::uuid) is `expected` (`None`: unless
    /// there is no current document).
    ///
    /// For a caller that read a document, let someone edit it, and is saving
    /// the edit: if another revision landed in between, the save is refused
    /// rather than silently replacing it. The uuid, not the id, because a
    /// copy renumbers ids: a caller that read the document from one file and
    /// saves into a copy of it still names the same revision.
    pub fn put_document_if_current(
        &mut self,
        kind: &str,
        name: &str,
        schema: &str,
        body: &str,
        expected: Option<&str>,
    ) -> Result<RevisionId> {
        self.transaction(|tx| tx.put_document_if_current(kind, name, schema, body, expected))
    }

    /// Delete document `(kind, name)` by writing a deletion revision, and
    /// return its id and uuid. The history is kept. `None`, and nothing
    /// written, when
    /// there is no current document to delete. A reserved or empty `kind`,
    /// or an empty `name`, is refused as [`put_document`](Self::put_document)
    /// refuses it.
    pub fn delete_document(&mut self, kind: &str, name: &str) -> Result<Option<RevisionId>> {
        self.transaction(|tx| tx.delete_document(kind, name))
    }
}

impl Transaction<'_> {
    /// [`ArchiveMut::put_document`] as part of this transaction.
    pub fn put_document(
        &self,
        kind: &str,
        name: &str,
        schema: &str,
        body: &str,
    ) -> Result<RevisionId> {
        put(&self.tx, kind, name, schema, body, None)
    }

    /// [`ArchiveMut::put_document_if_current`] as part of this transaction.
    pub fn put_document_if_current(
        &self,
        kind: &str,
        name: &str,
        schema: &str,
        body: &str,
        expected: Option<&str>,
    ) -> Result<RevisionId> {
        put(&self.tx, kind, name, schema, body, Some(expected))
    }

    /// [`ArchiveMut::delete_document`] as part of this transaction.
    pub fn delete_document(&self, kind: &str, name: &str) -> Result<Option<RevisionId>> {
        delete(&self.tx, kind, name)
    }

    /// Every document revision `src` holds, into this transaction's archive.
    /// See [`crate::rewrite::copy_documents_into`].
    pub(crate) fn copy_documents_from(&self, src: &Archive) -> Result<CopiedDocuments> {
        copy_documents(src, &self.tx)
    }
}
