//! Archive-level documents: the `documents` table (FORMAT.md §3.6).
//!
//! Everything else in an archive belongs to a source. A document belongs to
//! the archive: a saved view, a report's selection, an event that spans
//! every source. Revisions are appended and never changed, so a document
//! keeps its history, and a copy carries every revision with its identity.

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension};

use super::{Archive, ArchiveMut, Transaction};
use crate::error::{Error, Result};

/// Kinds that start with this are dendro's, and a caller's
/// [`put_document`](ArchiveMut::put_document) refuses them. dendro writes
/// none today; the prefix is held so it can later without colliding with a
/// caller's namespace.
pub const RESERVED_KIND_PREFIX: &str = "dendro.";

/// The table and its index, created with every new archive and, for an
/// archive from before the table, on the first document write. Both
/// statements are `IF NOT EXISTS` so the one text serves both.
pub(super) const DOCUMENTS_SQL: &str = "
CREATE TABLE IF NOT EXISTS documents(
  id INTEGER PRIMARY KEY,
  -- The revision's identity across files: minted at insert and carried by
  -- every copy, so a copy that meets a revision it already holds skips it.
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
CREATE INDEX IF NOT EXISTS documents_by_name ON documents(kind, name, id);
";

/// The current revision of one document: its newest, when that revision is
/// not a deletion. See [`Archive::document`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Document {
    /// The revision's row id in this file. Every copy renumbers it, so it is
    /// not an identity; it is what
    /// [`put_document_if_current`](ArchiveMut::put_document_if_current)
    /// compares against.
    pub id: i64,
    /// The revision's identity, carried by every copy.
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
    /// The revision's identity, carried by every copy.
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

fn ensure_documents(conn: &Connection) -> Result<()> {
    conn.execute_batch(DOCUMENTS_SQL)
        .map_err(Error::sqlite("failed to create the documents table"))
}

/// The newest revision of `(kind, name)`, deletion or not.
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

fn insert_revision(
    conn: &Connection,
    uuid: &str,
    kind: &str,
    name: &str,
    content: Option<(&str, &str)>,
    created_wall_ns: i64,
    supersedes: Option<i64>,
) -> Result<i64> {
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
/// current document's id is `e` (`Some(None)`: unless there is none).
fn put(
    conn: &Connection,
    kind: &str,
    name: &str,
    schema: &str,
    body: &str,
    expected: Option<Option<i64>>,
) -> Result<i64> {
    check_key(kind, name)?;
    if schema.is_empty() {
        return Err(Error::Message(format!(
            "document {kind}/{name} needs a schema naming its body's type"
        )));
    }
    if let Err(e) = serde_json::from_str::<serde_json::Value>(body) {
        return Err(Error::Message(format!(
            "document {kind}/{name}: the body is not JSON: {e}"
        )));
    }
    ensure_documents(conn)?;
    let newest = newest(conn, kind, name)?;
    if let Some(expected) = expected {
        let current = match &newest {
            Some(Revision::Put(d)) => Some(d.id),
            _ => None,
        };
        if current != expected {
            return Err(Error::DocumentConflict {
                kind: kind.to_string(),
                name: name.to_string(),
                expected,
                current,
            });
        }
    }
    let uuid = super::mint_uuid(conn)?;
    let created = wall_now_ns(conn)?;
    insert_revision(
        conn,
        &uuid,
        kind,
        name,
        Some((schema, body)),
        created,
        newest.map(|r| r.id()),
    )
}

fn delete(conn: &Connection, kind: &str, name: &str) -> Result<Option<i64>> {
    check_key(kind, name)?;
    if !has_documents(conn)? {
        return Ok(None);
    }
    let Some(Revision::Put(current)) = newest(conn, kind, name)? else {
        return Ok(None);
    };
    let uuid = super::mint_uuid(conn)?;
    let created = wall_now_ns(conn)?;
    insert_revision(conn, &uuid, kind, name, None, created, Some(current.id)).map(Some)
}

/// Every revision `src` holds, oldest first, into `dst`, skipping any whose
/// uuid `dst` already has. Returns how many were inserted.
///
/// `supersedes` is renumbered through the uuids, so it names the same
/// revision in `dst` that it named in `src`. Ids are assigned in `src`'s
/// order after everything `dst` already holds, which keeps `src`'s own
/// ordering, so the revision current in `src` is current in `dst` unless
/// `dst` already held a newer revision of the same document or a later copy
/// brings one.
fn copy_documents(src: &Archive, dst: &Connection) -> Result<usize> {
    if !has_documents(&src.conn)? {
        return Ok(0);
    }
    let revisions = all_revisions(&src.conn)?;
    if revisions.is_empty() {
        return Ok(0);
    }
    ensure_documents(dst)?;
    let mut ids: BTreeMap<i64, i64> = BTreeMap::new();
    let mut inserted = 0usize;
    for rev in &revisions {
        let (uuid, kind, name, content, created) = match rev {
            Revision::Put(d) => (
                &d.uuid,
                &d.kind,
                &d.name,
                Some((d.schema.as_str(), d.body.as_str())),
                d.created_wall_ns,
            ),
            Revision::Delete(d) => (&d.uuid, &d.kind, &d.name, None, d.created_wall_ns),
        };
        let held: Option<i64> = dst
            .query_row("SELECT id FROM documents WHERE uuid = ?1", [uuid], |row| {
                row.get(0)
            })
            .optional()
            .map_err(Error::sqlite("failed to look up a document revision"))?;
        if let Some(id) = held {
            ids.insert(rev.id(), id);
            continue;
        }
        // A pointer the source cannot resolve (a file this crate did not
        // write; ours are written superseding a row that exists) is carried
        // as unknown rather than failing the copy.
        let supersedes = match rev.supersedes() {
            None => None,
            Some(s) => match ids.get(&s) {
                Some(&mapped) => Some(mapped),
                None => {
                    tracing::warn!(
                        "document {kind}/{name}: revision {} supersedes {s}, which the source \
                         archive does not hold; copied as superseding nothing",
                        rev.id()
                    );
                    None
                }
            },
        };
        let id = insert_revision(dst, uuid, kind, name, content, created, supersedes)?;
        ids.insert(rev.id(), id);
        inserted += 1;
    }
    Ok(inserted)
}

fn all_revisions(conn: &Connection) -> Result<Vec<Revision>> {
    let mut stmt = conn
        .prepare(&format!("SELECT {COLUMNS} FROM documents ORDER BY id"))
        .map_err(Error::sqlite("failed to query documents"))?;
    let rows = stmt
        .query_map([], revision_from_row)
        .map_err(Error::sqlite("failed to query documents"))?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Error::sqlite("failed to read a document"))
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
    /// Write a new revision of document `(kind, name)` and return its id.
    ///
    /// The revision supersedes whatever was current, and becomes current
    /// itself; the earlier revisions stay in
    /// [`document_history`](Archive::document_history). Nothing is
    /// overwritten. `schema` names the body's type and version (say
    /// `"myapp.view/1"`) and is stored without being read. `body` must be
    /// JSON, and is refused otherwise; it is stored as given.
    ///
    /// `kind` and `name` must be non-empty, and a `kind` starting with
    /// [`RESERVED_KIND_PREFIX`] is refused. An archive from before the
    /// `documents` table gains it here, which changes no schema version
    /// (FORMAT.md §8).
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
    ) -> Result<i64> {
        self.transaction(|tx| tx.put_document(kind, name, schema, body))
    }

    /// [`put_document`](Self::put_document), refused with
    /// [`Error::DocumentConflict`] unless the current document's id is
    /// `expected` (`None`: unless there is no current document).
    ///
    /// For a caller that read a document, let someone edit it, and is saving
    /// the edit: if another revision landed in between, the save is refused
    /// rather than silently replacing it.
    pub fn put_document_if_current(
        &mut self,
        kind: &str,
        name: &str,
        schema: &str,
        body: &str,
        expected: Option<i64>,
    ) -> Result<i64> {
        self.transaction(|tx| tx.put_document_if_current(kind, name, schema, body, expected))
    }

    /// Delete document `(kind, name)` by writing a deletion revision, and
    /// return its id. The history is kept. `None`, and nothing written, when
    /// there is no current document to delete.
    pub fn delete_document(&mut self, kind: &str, name: &str) -> Result<Option<i64>> {
        self.transaction(|tx| tx.delete_document(kind, name))
    }
}

impl Transaction<'_> {
    /// [`ArchiveMut::put_document`] as part of this transaction.
    pub fn put_document(&self, kind: &str, name: &str, schema: &str, body: &str) -> Result<i64> {
        put(&self.tx, kind, name, schema, body, None)
    }

    /// [`ArchiveMut::put_document_if_current`] as part of this transaction.
    pub fn put_document_if_current(
        &self,
        kind: &str,
        name: &str,
        schema: &str,
        body: &str,
        expected: Option<i64>,
    ) -> Result<i64> {
        put(&self.tx, kind, name, schema, body, Some(expected))
    }

    /// [`ArchiveMut::delete_document`] as part of this transaction.
    pub fn delete_document(&self, kind: &str, name: &str) -> Result<Option<i64>> {
        delete(&self.tx, kind, name)
    }

    /// Every document revision `src` holds, into this transaction's archive.
    /// See [`crate::rewrite::copy_documents_into`].
    pub(crate) fn copy_documents_from(&self, src: &Archive) -> Result<usize> {
        copy_documents(src, &self.tx)
    }
}
