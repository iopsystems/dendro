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
  revision's uuid with the one the caller read and refuses with
  `Error::DocumentConflict`. That is the concurrency answer for "a viewer
  read the document, the user edited it, the viewer saves". The first build
  compared ids; a review pointed out that a copy renumbers them, so an
  editor that read from one file and saved into a combined copy got a false
  conflict. The token is the uuid, and the conflict carries uuids.
- **A `uuid` per revision.** Not in the original sketch. Without it a copy
  cannot tell a revision it already holds from a new one, so combining an
  archive with a copy of itself, or copying documents twice into one
  destination, would duplicate every revision and fork every document. With
  it a copy is idempotent, and `supersedes` is renumbered through it.
- **The same uuid with different contents fails the copy.** A revision is
  never changed, so a held revision that differs in kind, name, schema or
  body from the incoming one means one of the two files is wrong. Skipping
  it would keep whichever arrived first without saying so; a warning would
  do the same in a log nobody reads. The copy cannot tell which file is
  right, so it refuses. `created_wall_ns` and `supersedes` are not compared:
  the first is provenance, the second a file-local id.
- **A copy reports what it changed (`CopiedDocuments`).** rezolus combines
  archives that each carry a `default` view, and needs to know when one
  input's view displaced another's. `copy_documents_into` returns the
  revisions inserted, the documents whose current revision came from this
  input, and `forked`: those where the destination's newest revision before
  the copy is not an ancestor (through `supersedes`) of the new one. Each
  fork is a `Fork` with both revisions' uuids, so a combine can rename the
  displaced one. A newer copy of the same document is an update, not a
  fork. A deletion counts as the baseline when the incoming chain shares a
  revision with the destination's, so a delete in one input and an edit in
  another are a fork in either order (the first build ignored deletions
  there, and the delete-then-edit order resurrected the document without a
  report); a document the destination had deleted, followed by an
  unrelated one under its name, is not a fork. The baseline is captured
  before the copy inserts anything for the document, not recomputed as its
  revisions land. `copy_sources_into` still returns the source count: a
  caller that wants the report calls `copy_documents_into` first, inside
  the same `src.read_snapshot`, and the copy inside `copy_sources_into` then
  inserts nothing. Returning the report from `copy_sources_into` was
  considered and not done, because it would change the return type at
  every caller for the one caller (a combine) that needs it.
- **The current revision is not chosen by input.** A review constructed
  the case: the destination holds A then B (current B); the input holds A,
  C, B, also current B, and the copy inserts only C, which becomes current
  because it is newest. Making the input's head current instead would need
  either a pointer that is not derived from the rows, or a restatement
  revision that neither file contains and that a repeated copy would have
  to recognize by content. The id rule stays, `forked` names the document
  (C does not supersede B), and the docs say so: a shared document's
  current revision is the newest one the copy inserted.
- **A write returns its revision (`RevisionId`).** `put_document`,
  `put_document_if_current` and `delete_document` return the new
  revision's id and uuid, so a caller can chain conditional saves without
  reading the document back between them, which would leave a window for
  another write.
- **`schema` is the caller's and never read.** It is the body's type name
  and version (`rezolus.view/1`). dendro cannot interpret a body, so it
  cannot version one; what it can do is store the version beside it and
  state the rule for readers (FORMAT.md §3.6): do not interpret a body whose
  `schema` you do not know, and you may walk the history back to the newest
  revision you do know. Cross-version compatibility of the body is then a
  consumer decision with the information it needs in the file.
- **`body` must parse as JSON, checked without building it.** The first
  build parsed into `serde_json::Value`, which a review measured at 528 MB
  peak for one 56 MB body, and which refuses two kinds of valid JSON:
  nesting past 128 and an escaped lone surrogate (`"\ud800"`, which
  JavaScript's `JSON.stringify` produces). `serde_core::de::IgnoredAny` walks the
  text and keeps nothing, has no nesting limit, and accepts the surrogate;
  it still refuses trailing characters, control characters, bad escapes,
  `NaN` and comments (probed on serde_json 1.0.151). SQLite's `json_valid`
  was the other candidate: it accepts the surrogate and refuses nesting past
  1000, so a deep but valid body would still be refused. `serde_core`, which
  serde_json 1.0.151 is built on, is a direct optional dependency under
  `archive`; `serde` itself is not in the normal dependency graph.
- **Hold one body at a time.** A put reads the previous revision's id and
  whether it is a deletion, not its body. A copy reads the source with one
  statement, row by row, binding each row's text straight into the insert,
  and compares a held revision in SQL. That one statement is also the copy's
  snapshot of the source, so `copy_documents_into` no longer opens a read
  transaction of its own.
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
- **dendro does not enforce principle 2.** A producer's template and a
  user's layer are two documents (`name` "producer" and "user", or two
  kinds) that the consumer composes. dendro guarantees the part it can: no
  write replaces another revision, so the producer's copy survives a user's
  edit even under the same name.

### Compatibility: schema version 5

The first build of this change kept schema version 4 and created the table
lazily, on the argument in FORMAT.md §8 that an old reader would not misread
a new table. That argument was checked in the code of origin/main
(`da79f94`, the v0.5.1 code) and then run, with two scratch binaries, one on
each build, exchanging files:

- The open gate (`adopt_schema`) reads `application_id` and `user_version`
  from the header and nothing else.
- `verify` runs `integrity_check`/`quick_check` and `foreign_key_check`
  over the whole file, both of which pass a well-formed extra table, then
  walks `sources` and their streams.
- The only table listing is `user_table_names`, `#[cfg(test)]`.
- `copy_sources_into` copies a fixed set of tables.

| Step (first build, v4 with a lazy table) | Result |
| --- | --- |
| old build reads an archive before and after this build added a document | identical output: sources, segment bytes, WAL rows, caller rows, `verify(Full)` sound |
| old build: `ArchiveMut::open`, then `Writer::open` + `resume_source` + append + finalize | succeeds; the document is intact afterwards |
| old build reads an archive this build wrote with revisions and a deletion | no refusal; everything intact; `verify(Full)` sound |
| old build `copy_sources_into` of it | 1 source copied, **0 documents** |
| old build `vacuum_into` of it | every revision |

The reader half held. The owner decided (2026-10-19) to bump anyway,
because of the copier: an old `copy_sources_into` drops the table, and a
document is something a person wrote, not a fact that degrades to
"unknown". FORMAT.md §8 now gives that reason. The bump is at create:

- Every archive this build creates is stamped 5 and has the table.
- The gate reads 4 and 5 (`OLDEST_READ_SCHEMA_VERSION`); 0.5.1 refuses 5.
- A v4 archive reads as holding no documents.
- A lazy upgrade on first write (the reviewer's suggestion) was not taken:
  §8 says an archive is never migrated in place. A document written into a
  v4 archive, by a put or by a copy that would insert a revision, is
  refused as `Error::SchemaTooOld`, and the remedy is a copy-forward
  through `rewrite` into a new archive, which is v5.
- Everything else a v4 archive holds is still written in place, and the
  file stays v4: a writer resumes its sources, metadata and labels are
  edited, compaction and retention run. That is a reading of §8's "writes
  only its own" as "adds nothing of its own version to an older archive",
  rather than "refuses to write an older archive at all". The strict
  reading would break `rezolus recording annotate`, which edits a `.dendro`
  in place through `ArchiveMut::open`, on every recording made before the
  upgrade, and would stop a recording from being resumed across a dendro
  upgrade. rezolus's hindsight creates its buffer rather than reopening
  one, so it is unaffected either way.

Run again with the bump in place (0.5.1 against this build):

| Step | Result |
| --- | --- |
| 0.5.1 writes an archive; this build reads it | read as v4; sources, segment bytes, WAL, caller rows intact; `verify(Full)` sound |
| this build `put_document` into it | `SchemaTooOld { found: 4, needs: 5 }` |
| this build copies it forward (`copy_sources_into` into a new archive) and puts a document there | new archive is v5, the source keeps its uuid, the document reads back, `verify(Full)` sound |
| 0.5.1 resumes and appends to the original afterwards | succeeds: it is still v4 |
| 0.5.1 opens an archive this build created | `UnsupportedSchema { found: 5, writes: 4, reads: 4 }` |

(The copy-forward's live tail went through the probe's encoder, which
encodes nothing, so its one WAL row is not in the copy. That is the copy's
existing rule for the tail, not something this change touched.)

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
| `rewrite::copy_documents_into` | the same, with a report; for an assembly that builds its sources another way, or a combine that must detect forks |
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
one document (two edited copies of one report) the history holds both edits,
the newest revision the later copy inserted is current, and that copy's
`CopiedDocuments::forked` names the document. A copy into a v4 destination is refused when the source holds any
revision.

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

Implemented on `feat/archive-documents`: the first build (`6511d10`), the
review fixes with the schema bump (`e5d51c7`), and the second review's
fixes (`4fc4f26`). The same branch also carries the segment index table
and the encoder context ([their entry](2026-10-19-segment-index-table.md)).

Tests: 22 in `tests/documents.rs` (20 of them also in the reader-only
build), `compaction_keeps_the_archives_documents` in `tests/compaction.rs`,
and `the_schema_before_this_one_opens_and_the_one_before_that_does_not` in
`tests/identity.rs`.

Mutation checks, one command each (mutate, run, restore, and a hash check
that every touched file was restored):

- First build: 25 mutations. 24 turned at least one test red. The 25th,
  removing the `body IS NOT NULL` filter from `documents()`'s SQL, changed
  nothing, because the Rust loop also drops deletions. The SQL filter was
  removed; mutating the Rust filter turns
  `listing_shows_each_current_document_of_a_kind_once` red.
- The review then found five more mutations that the tests let through:
  - a copy losing `schema`, and a copy restamping `created_wall_ns`, both
    because the test helper compared only `(uuid, body)`;
  - a seconds-resolution clock;
  - a delete without the key check;
  - `copy_documents_into` without its read snapshot.

  The helper now compares `(uuid, schema, body, created_wall_ns)`, with a
  sleep before the copy so a restamp differs. New tests cover the clock's
  resolution and the delete refusals. The snapshot wrapper was removed
  rather than tested: the copy now reads the source with one statement,
  which is one SQLite read transaction, and no test could interleave a
  write into one statement deterministically.
- Second build: 40 mutations, the first set re-anchored plus the review's
  survivors, the validator, the copy report, the content check and the v4
  rules. All 40 turned at least one test red, and every test was red
  under at least one of them. A second review then found two more that
  the tests let through: recomputing the fork baseline at each insert, and
  a writer open that creates the table in a v4 archive. Two tests were
  added for them: a fork against a destination that holds A then B, and a
  `sqlite_master` check in the writer v4 test.
- Third build: 53 mutations, the 40 re-anchored plus those two, the
  deletion baseline, the shared-ancestor rule, the `Fork` uuids, the
  returned `RevisionId`, and a v4 restamp on writer open. All 53 turned at
  least one test red.

Memory, peak RSS by `/usr/bin/time -l`, one run each, release build, same
harness and bodies (an array of short strings):

| Case | first build | now |
| --- | --- | --- |
| put a 56 MB body, previous revision 1 MB | 410 MB | 261 MB |
| put a 56 MB body, previous revision 56 MB | 469 MB | 260 MB |
| copy 10 documents of 56 MB | 735 MB | 207 MB |

What remains in the put is the caller's own body, SQLite's bound copy of
it, and the read cache `ArchiveMut::open` sets (256 MiB). The copy no
longer grows with the number of documents.

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
