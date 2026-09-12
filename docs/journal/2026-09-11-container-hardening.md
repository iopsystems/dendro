---
status: in-progress
opened: 2026-09-11
updated: 2026-09-11
---

# Container hardening: failure classes, identity, sessions, and one materialization

## Goal

Close the gaps a second critical review found between what the container
promises and what a caller can rely on, in the order that each unblocks the
next. The review re-read the crate after the four open entries beside this one
were written, and carried over a set of changes that had already been built and
tested against the identical schema in rezolus's `crates/rez` (rezolus PR
#1201, closed without merging once the format moved here).

The design is not in question. Sources, streams, opaque rows behind an encoder,
a WAL inside the file, the seal seam by watermark: all of it holds up, and the
four existing entries name the boundaries honestly. What this entry covers is
the layer *around* that design — what a failure is, what a source is, who wrote
it, and whether a reader and the writer agree about a tail.

## Decision Criteria

Each item lands with the test named beside it, and the three CI configurations
(`--features test-support`, `--no-default-features` natively, and the wasm32
check) stay green. No item changes the segment layout, the seal policy, or the
tick path's transaction count; the one performance-sensitive change (retries on
the writer thread) is bounded to ~310 ms per failing message and stated as
such.

**NO-GO** on any item that needs the container to learn what a row means. The
producer epoch that motivated identity work in rezolus is *carried* here as a
reserved metadata convention and a writer primitive; it is not detected here.

## Scope

In, ordered:

1. **SQLite result codes survive into `Error`.** `Error::Sqlite` is declared
   and never constructed; every statement in `db.rs` stringifies into
   `Error::Message`, so a lock, a full disk, and a primary-key collision are
   the same error. Add context-carrying `Sqlite { context, source }`,
   `sqlite_code()`, `is_retryable()`, `is_constraint()`.
2. **The writer classifies instead of stopping.** Retryable failures are
   retried on a bounded schedule; a tick that still fails is dropped, and a
   run of drops stops the writer; a seal that still fails is deferred (its
   rows are still live); a constraint on a batched tick is re-committed per
   source so only the colliding source loses rows; `seq` advances only after
   a seal commits; a reclaim failure after a successful eviction no longer
   kills the writer; an encoder panic on the writer thread is reported as
   the encoder's error rather than `WriterGone`. This is the [blast
   radius](2026-09-11-writer-failure-blast-radius.md) entry's "what isolated
   means when the transaction is shared", answered.
3. **Identity.** `PRAGMA application_id` and `user_version` stamped at
   create and checked before any other pragma on every open, with
   `Error::NotAnArchive`; a nullable `sources.uuid` minted at insert (from
   SQLite's `randomblob`, so the reader build needs no random source),
   carried verbatim by every copy, and a `rewrite` helper that refuses to
   copy a source whose uuid the destination already holds.
4. **Metadata during a recording.** A writer message that patches a source's
   metadata in order with its ticks, so a caller can persist what it learns
   per tick — an epoch change, a discontinuity — without waiting for a
   finalize a kill never reaches. Reserved keys (`producer_epoch`,
   `producer_epochs`, `writer_sessions`, `events`) documented as
   conventions.
5. **Reopen for append.** `Archive::open` over an existing file, seeding
   `next_seq` from the catalog; `resume_source` that refuses an anchor at or
   before the source's newest row, clears `complete`, records a writer
   session, and returns a handle that refuses any earlier tick.
6. **One materialization.** The writer, the copy, and the read path each run
   the encoder over a tail and validate the result differently; the read
   path's check is the weakest. One shared function with one contract.
7. **Per-stream retention bounds `clock_offsets`**, as whole-source retention
   already does.
8. **A container spec** (`FORMAT.md`): catalog column semantics, the
   watermark rule, the clock model, sessions, and what bumps
   `SCHEMA_VERSION` versus what is additive.

Out: the lazy catalog read that rezolus built above this crate (a design of
its own, with its own measurements to redo); the encoder version marker
(named in [the encoder boundary](2026-09-11-encoder-boundary.md); it should
ride on the same `UpdateMetadata` primitive once that exists, and is a
one-line follow-on); backfill and compaction, which have their own entries.

## Evidence

Review of `f4a2d6c`, 2026-09-11, two independent passes plus a re-read of
every high finding:

- `grep -c 'Error::Sqlite' src/db.rs` is 0; every `map_err` in the file
  builds `Error::Message(format!(...))`.
- `writer_loop` (`src/writer.rs`): `Msg::Wal` and `Msg::Seal` are a bare `?`;
  `reclaim_if_fragmented(db)?` follows an eviction whose result was already
  replied to the caller. No retry anywhere in the crate.
- `seal_batch` bumps `seq` before the commit, with the comment "safe only
  because the writer exits on its first error".
- `sources` is `id, labels, metadata, complete, clock_anchor_wall_ns`;
  `copy_sources_into` inserts a fresh row per copy. No `application_id`;
  `adopt_schema` reads `schema_version` *after* `apply_connection_pragmas`
  has written `synchronous` and friends.
- `Db::update_source_metadata` exists; nothing in `writer.rs` reaches it.
- `read::stream_segments_snapshotted` checks `first_ts` and a row-count
  upper bound; `seal_batch` and `rewrite` check contiguity by counting.
- `evict_streams_before` deletes segments and WAL rows only; `evict_before`
  also deletes `clock_offsets`.
- `writer_thread` stores the error slot only on `Err`; an unwinding encoder
  reaches every handle as `WriterGone`.

The rezolus implementation of items 1–5 (PR #1201, commits `c4ce2d26`,
`0db2f3ed`, `81314e3b`, `600d0ed3`, `45b6f853`, `9b043a12`) passed its crate
suite and two negative controls: the seam test read 0 of 3 rows on the
two-statement read, and both writer-policy tests failed on the single-`?`
commit path. Those controls are repeated here as each item lands.

## Design and Implementation

Recorded per item as it lands, below the plan.

## Outcome

In progress.

## Deferred or Reopen Items

- **Lazy catalog read.** `read_archive` materializes every BLOB of every
  stream; `SegmentBytes` exists and nothing produces one. Reopen with the
  first consumer that opens an archive wider than it reads.
- **Encoder version marker.** One reserved key written at `add_source`,
  compared on read, refused on mismatch. Reopen once item 4's primitive
  exists; a one-line follow-on.
- **Retention versus encoder anchors.** Eviction by timestamp can delete the
  row a later row needs to decode; the dropped run is then pruned into
  nothing. Options: the encoder declares its oldest self-sufficient row, or
  eviction never deletes live WAL rows and reports the ones it kept. Reopen
  with a caller whose lookback is shorter than its seal age.
- `Db`'s mutators taking `&self`, `SQLITE_OPEN_URI` on `open_read_only`
  only, `source_time_span` counting shadowed rows, `reclaim_all` uncapped in
  `Drop`, fixed writer props in `project_segment_columns`, fleet constants as
  `SealPolicy` defaults: all small, all real, none blocking.

## Appendix: Skills Invoked

- `engineering-journal` — this entry.
