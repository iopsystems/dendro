---
status: implemented
opened: 2026-10-19
updated: 2026-10-19
---

# Archive documents: a place for what belongs to the whole archive

## Goal

Give an archive somewhere to keep things that belong to the archive rather
than to one of its sources, with history, so that a view of an archive can
be saved in the archive and a user's additions never overwrite what the
producer wrote.

## Why

Everything in an archive besides the data is per source: `sources.labels`,
`sources.metadata`, `caller_rows`. rezolus stores a saved view, a report's
pinned selection, archive-level events and user template layers, and with
nowhere archive-level to put them it put them in one recording's metadata.
That caused a series of bugs about which recording "carries" the report:
the choice depended on source order, a filter that dropped that source
dropped the report, and a combine of two archives had two candidates.

The owner's principles (2026-10-19):

1. Views of an archive must be preservable in the archive.
2. User additions are additive layers, never overwrites of the producer's
   copy.
3. History and versioning: a document keeps its past revisions, and a
   document body written by one version of a consumer is identifiable to
   another.

## Design

One table, `documents` (FORMAT.md §3.6; DDL in
`src/archive/sqlite/documents.rs`, `DOCUMENTS_SQL`). Each row is a revision
of the document `(kind, name)`: `uuid`, `schema`, `body` (JSON),
`created_wall_ns`, `supersedes`. The decisions, with the reason for each:

- **Append-only, and a delete is a row.** A deletion is a revision with
  `schema` and `body` both `NULL` (a `CHECK` keeps them together). Removing
  rows would lose history, which principle 3 forbids. A put after a delete
  continues the chain from the deletion.
- **Current is the newest by `id`, not the head of the `supersedes`
  chain.** Chain-following needs a tie-break for forks anyway, and `id` is
  the archive's insertion order, assigned inside the writing transaction.
  `supersedes` is provenance: it records which revision the writer replaced,
  so a fork (two revisions superseding one) is visible in
  `document_history`, and nothing is lost when one happens. A writer that
  must not fork uses `put_document_if_current`, which compares the current
  id with the one the caller read and refuses with
  `Error::DocumentConflict`. That is the concurrency answer for "a viewer
  read the document, the user edited it, the viewer saves".
- **A `uuid` per revision.** Not in the original sketch. Without it a copy
  cannot tell a revision it already holds from a new one, so combining an
  archive with a copy of itself, or copying documents twice into one
  destination, would duplicate every revision and fork every document. With
  it a copy is idempotent, and `supersedes` is renumbered through it.
- **`schema` is the caller's and never read.** It is the body's type name
  and version (`rezolus.view/1`). dendro cannot interpret a body, so it
  cannot version one; what it can do is store the version beside it and
  state the rule for readers (FORMAT.md §3.6): do not interpret a body whose
  `schema` you do not know, and you may walk the history back to the newest
  revision you do know. Cross-version compatibility of the body is then a
  consumer decision with the information it needs in the file.
- **`body` must parse as JSON.** Checked at write with `serde_json`; nothing
  else reads it. A reader can then rely on the column holding JSON.
- **`kind` is the caller's namespace; `dendro.` is reserved.** dendro writes
  no documents today. Reserving the prefix now, and refusing it from
  callers, means a later dendro can write archive-level documents of its own
  without colliding with anything an existing archive holds.
- **`created_wall_ns` from SQLite's clock.** `std::time::SystemTime::now`
  panics on `wasm32-unknown-unknown`, and `ArchiveMut::create_in_memory` is
  how a browser assembles a report archive. SQLite's `unixepoch('subsec')`
  comes from its VFS, which works there, the same reason `mint_uuid` uses
  `randomblob`. Millisecond resolution. Provenance only; no rule orders by
  it.
- **Principle 2 is a naming convention, not a mechanism.** A producer's
  template and a user's layer over it are two documents (`name` "producer"
  and "user", or two kinds); the consumer composes them. dendro guarantees
  the part it can: no write replaces another revision, so the producer's
  copy survives a user's edit even under the same name.

### Compatibility

FORMAT.md §8 says a change an old reader would not misread needs no schema
bump. A new table qualifies only if no existing reader, copier or validator
lists tables or refuses unknown ones, so that was checked in the code of
origin/main (`da79f94`, the v0.5.1 code):

- The open gate (`adopt_schema`) reads `application_id` and `user_version`
  from the header and nothing else.
- `verify` runs `integrity_check`/`quick_check` and `foreign_key_check`
  over the whole file, both of which pass a well-formed extra table, then
  walks `sources` and their streams.
- The only table listing is `user_table_names`, `#[cfg(test)]`, used by
  `rewrite`'s schema-coverage test.
- `copy_sources_into` copies a fixed set of tables, so the previous release
  drops documents from a copy. That is the "old copiers drop what they do
  not know" case.

Then it was run. Two scratch binaries, one against origin/main's dendro and
one against this branch, exchanged files (not committed; the transcript is
summarized here):

| Step | Result |
| --- | --- |
| old build writes an archive; new build adds a document through `ArchiveMut::open` | table created on the write |
| old build reads that archive before and after | identical output: sources, segment bytes, WAL rows, caller rows, `verify(Full)` sound |
| old build: `ArchiveMut::open`, then `Writer::open` + `resume_source` + append + finalize | succeeds; new build then reads the document unchanged, and the old session's row |
| new build writes an archive with two revisions and a deletion; old build reads it | no refusal; sources, segments, WAL, caller rows intact; `verify(Full)` sound |
| old build `copy_sources_into` of it | 1 source copied, 0 documents |
| old build `vacuum_into` of it | every revision, deletion included |

So the schema version stays 4. Creating the table lazily is safe under both
write handles: `ArchiveMut::open` holds the file exclusively, and the
writer thread is the file's one writing connection; the DDL runs inside the
write's own transaction, so a failed write leaves no table. The read
handle's `query_only` never needs to create anything, because every read
checks `sqlite_master` first and answers "no documents".

### Writing during a recording

`ArchiveMut::open` is refused as `InUse` while a `Writer` holds the file,
and there is one writing connection by design. A document written while a
recording runs (a live view saving a report, hindsight annotating its
buffer) therefore goes through the writer thread: `Writer::put_document`,
`put_document_if_current` and `delete_document` send a message and wait for
the committed id. A refusal is returned to the caller and does not stop the
writer. A second process cannot write documents into an archive a writer in
another process holds; that is the same rule as every other write.

### Copies

| Path | Documents |
| --- | --- |
| `rewrite::copy_sources_into` | every revision, regardless of `CopySpec`'s time bound or stream filter |
| `rewrite::copy_documents_into` | the same, for an assembly that builds its sources another way |
| `Archive::vacuum_into`, `serialize` | the whole file |
| `rewrite::compact` | untouched (tested) |
| retention | untouched |
| replication | not carried |

Documents are archive-level, so no part of `CopySpec`, all of which selects
by time or stream, selects them. A `CopySpec` field to opt out was not
added: the struct is not `#[non_exhaustive]`, so a new field breaks any
caller that builds it without `..CopySpec::everything()`, and no caller has
asked to drop documents. A combine calls `copy_sources_into` once per input,
so it carries each input's documents; where two inputs hold revisions of
one document (two edited copies of one report) the history holds both edits
and the input copied last is current.

## Alternatives not taken

- **A reserved key in one source's metadata.** What rezolus did, and the
  cause of the bugs above.
- **A pseudo-source holding only documents.** Every per-source reader would
  have to learn to skip it, and an old reader would show it as a source.
- **Mutable rows with a separate history table.** Two tables to keep in
  step, and an old copier that knew one would carry half a document.
- **Current as the head of the `supersedes` chain.** Needs a fork rule, and
  `id` order is that rule; see the design.

## Outcome

Implemented on `feat/archive-documents`. Tests in `tests/documents.rs`
(13) and `tests/compaction.rs` (`compaction_keeps_the_archives_documents`),
each mutation-checked: 25 mutations of the implementation, each turning at
least one test red. One mutation survived the first run: removing the
`body IS NOT NULL` filter from `documents()`'s SQL changed nothing because
the Rust loop also drops deletions. The SQL filter was removed; mutating the
Rust filter turns `listing_shows_each_current_document_of_a_kind_once` red.

The "previous release" archive in the committed tests is a new archive with
`documents` dropped through a raw connection. That is exact for this change
because the table and its index are the only schema difference; the real
cross-build check is the table above.

## Reopen

- **Replication of documents.** Open if a subscriber's archive needs the
  publisher's documents. It needs an archive-level frame kind, which WIRE.md
  §7 allows without a protocol version bump (an unknown kind is skipped by
  subscribers from 0.3.0), and a rule for when the publisher sends them.
- **Purge.** No API removes a revision. Open if a document must be erased
  (a secret pasted into a body); it would be the one exception to
  append-only and should leave a deletion behind.
- **Cross-process writes to a live archive.** Open if a process other than
  the recorder must save a document while it records. Today it writes to a
  copy.
- **`verify` of documents.** It checks `supersedes` references through
  `foreign_key_check` only. Open if copies from other implementations
  appear: a `supersedes` naming a revision of another `(kind, name)`, or a
  later `id`, is not reported.
