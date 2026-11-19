---
status: open
opened: 2026-11-18
updated: 2026-11-19
---

# Audit: the file format, the SQLite schema, and how dendro uses SQLite

## Scope

An audit of the storage layer as it stands at a7000b5, not of a change.

The question: is the schema sound for the rules FORMAT.md gives it? Is
SQLite's WAL mode used correctly (durability, checkpoints, snapshots,
connection modes)? Do the write paths keep the catalog consistent through
failure and restart?

In scope:
- `src/archive/sqlite.rs`, `src/archive/sqlite/documents.rs`,
  `src/archive/lock.rs`;
- the storage paths of `src/writer.rs`;
- `src/segment.rs`, `src/seal.rs`, `src/read.rs`;
- the catalog writes in `src/rewrite.rs`;
- FORMAT.md and DESIGN.md, read as claims about the code.

Out of scope: `src/replicate/*`, WIRE.md, and the writer's replication-only
paths (`adopt_segment*`, `sync_stream`, replica poisoning).

Five reviewers ran in parallel, one per concern: schema design, write-path
transactions, WAL and connection use, failure and restart, and docs against
code. Each one built and ran probes in a scratch copy. Every finding marked
confirmed below was either reproduced again in a fresh worktree at a7000b5
or traced through the code by the auditor. Reproduced: findings 1, 2, 3, 4,
6, 7 and 13, and docs items d1–d3. Traced: findings 5, 8 and 9. Findings
14–18 and docs items d4–d13 rest on the reviewers' runs or reading and were
not re-checked by the auditor.

## Invariants

The owner agreed these before review. Ones marked *inferred* were derived by
the auditor, not taken from the docs.

1. Every created file carries `application_id` and `user_version` in the
   main file from creation. Every open refuses other ids and versions before
   it writes anything.
2. A committed tick is durable. An unclean kill loses at most the tick
   queued to the writer and the one mid-commit, and never a committed row.
3. A seal's segment inserts, clock observation and covered-WAL delete commit
   together. A failed seal leaves the stream as it was, with no `seq` hole.
4. In one snapshot, a stream reads as its segments followed by the rows
   above `MAX(last_ts)`, with no duplicate and no gap.
5. Within a stream, `seq` order is time order, on every dendro write path.
6. Within each `(source, stream)`, `ts` strictly increases for the life of
   the source. `bounds` is written before each delete, in the same
   transaction, and is never lowered.
7. Retention never deletes a live row and never un-shadows a covered row.
8. No `segment_indexes` row outlives its segment. *Inferred:* every
   connection that deletes segments has `foreign_keys=ON`.
9. `clock_offsets` holds at most one row per `(source, ts)`, and each row is
   a real row's `(ts, wall_offset)` pair.
10. *Inferred:* the writer's `next_seq`, watermarks and newest-row caches
    match the file after every commit, including failed and retried ones.
11. One writing connection, enforced by `-lock`. A read handle on an
    unfinished archive writes nothing.
12. The `-wal` sidecar is bounded by the 4 MiB autocheckpoint plus the 10 s
    checkpoint, absent a held snapshot. The main file stays within its
    high-water mark under retention.
13. A v5 build adds no v5 structure to a v4 file and never restamps one.
14. *Inferred:* keys, constraints and indexes match FORMAT's rules, and
    every hot-path query is served by an index.
15. FORMAT.md and DESIGN.md state what the code does.

## Verdicts

| Invariant | Verdict | Evidence |
|---|---|---|
| 1 | plausibly broken | finding 10: a kill during create leaves a file with no stamp (traced, not staged). Finding 1's refused open writes, but `-lock` refuses it, not the id or version check |
| 2 | broken | finding 1 (committed rows lost); finding 11 on Apple; finding 5 (loss bound under a failing disk) |
| 3 | holds | one transaction; `seq` advances after commit; a failed seal left seqs 0..16 contiguous |
| 4 | holds | writer, retention and copy paths traced; retention control test below |
| 5 | holds | seal, `split_seal`, compaction and copy traced |
| 6 | holds | every delete of a stream's newest row records `bounds` first, in the delete's transaction |
| 7 | holds | control test: live set `[11, 12]` unchanged at cutoffs 5, 7, 11 and 100 |
| 8 | holds | `foreign_keys=ON` on every writing connection; the bundled build also defaults it on |
| 9 | broken as worded | finding 16: `finalize` and `clock_offset` store the caller's pair |
| 10 | holds for tick, seal and evict | a failed add or resume leaves the file changed (finding 8) |
| 11 | broken | findings 1, 2 and 12 |
| 12 | broken | findings 4 and 9 |
| 13 | holds | v4 probe: `user_version` stays 4, and only `bounds` and the heartbeat columns are added |
| 14 | holds, with exceptions | findings 17 and 18; docs item d10 |
| 15 | broken | docs list below |

## Disposition (owner, 2026-11-19)

None of the findings will be fixed on the current SQLite format. The owner
decided to pursue the append-only single-file container
([2026-11-18-single-file-log-container.md](2026-11-18-single-file-log-container.md))
instead. That entry carries every finding as a requirement on the new
format, under "Requirements carried from the audit".

The auditor recommended fixing findings 1 and 2 on the current format
anyway, and the owner chose not to.

- **Finding 1** loses committed rows today, when a second `ArchiveMut::open`
  runs in the writer's own process.
- **Finding 2**'s read path outlives the switch: a copy-forward still reads
  v4 and v5 files.
- **Exposure until the new format ships:** both findings stay live, and the
  workaround for finding 1 is to never open a second write handle in a
  recording process.

**Reopen** any finding on the current format if the single-file effort
reaches NO-GO or stops, or if finding 1 or 2 is reported from use.

| Finding | Where it lands |
|---|---|
| 1, 2, 3, 4, 10, 11, 12, 17, 18 | SQLite-specific. Each becomes a property the new container must hold by construction, listed in the container entry. |
| 5, 6, 7, 8, 9, 13, 14 | Writer logic, not SQLite. A new container does not fix them by itself, so each is a requirement with a test in the new writer. |
| 15, 16 | Design questions (below). They are decided in the new format's design. |
| d1–d13 | Docs of the current format. They are rewritten with the new format's FORMAT.md, and not patched before. |

## Findings

Ranked by consequence. Status is confirmed, plausible or refuted. Each
finding's disposition is under "Disposition" above.

### 1. A refused `ArchiveMut::open` can delete a live writer's `-wal`

- **Status:** confirmed, reproduced.
- **Location:** `sqlite.rs:148` (`sniff`) and `sqlite.rs:932`, then
  `wrap_locked`.
- **What happens:**
  1. On unix, `sniff` opens and closes the archive with `std::fs`. Closing
     any descriptor on a file releases every POSIX lock the process holds on
     it, so a writer in the same process loses SQLite's SHARED lock.
  2. `ArchiveMut::open` in another process then gets SQLite's exclusive
     lock, passes the gate, and re-enables checkpoint-on-close.
  3. That open is then refused by `-lock`, which is correct. Its close,
     however, checkpoints and unlinks the live `-wal`.
- **Probe:** a writer in a child process committed 3 rows, then 3 more,
  then was killed. Without `sniff`, 6 of 6 rows survived. With `sniff`, the
  `-wal` disappeared after the refused open, and 3 of 6 committed rows
  survived.
- **Proposed fix:**
  - take `-lock` before SQLite's open, or at least before checkpoint-on-close
    is re-enabled;
  - separately, read the header in `sniff` without closing a descriptor on
    the file in a process that may hold it open, or document the hazard.

### 2. The `immutable=1` fallback can return corrupt reads

- **Status:** confirmed, reproduced; the trigger was simulated with `chmod`.
- **Location:** `sqlite.rs:999-1012`.
- **What happens:**
  - `is_readonly_media` accepts any `SQLITE_READONLY` or `SQLITE_CANTOPEN`.
    A directory that this reader cannot write and another user's writer can
    counts as read-only media.
  - Once a writer appears and checkpoints, the immutable handle reads pages
    that were rewritten under it.
- **Probe:** `verify(Quick)` was unsound on 300 of 300 passes ("Tree 5 page
  5 right child: invalid page number 26", "database disk image is
  malformed").
- **Proposed fix:** fall back to `immutable=1` only when the filesystem
  itself is read-only (`statvfs` `ST_RDONLY`). Otherwise return the error.

### 3. Copies from `vacuum_into` and `serialize` are rollback-journal files

- **Status:** confirmed, reproduced.
- **Location:** `sqlite.rs:1979` (`VACUUM INTO`) and `create_in_memory`.
  `set_journal_mode_wal` is called only from `init_created`.
- **Probe:** the copy has `journal_mode=delete` (header bytes 18–19 are
  `[1, 1]`). It stays in `delete` mode while a `Writer` appends to it.
  Images from `create_in_memory` also have `auto_vacuum=0`.
- **Consequence:** in rollback mode a reader's SHARED lock blocks the
  writer's commit. The writer then waits `busy_timeout`, retries, and drops
  ticks.
- **Proposed fix:** set `journal_mode=WAL` in `open_for_write` and
  `ArchiveMut::open` when the file is not already in WAL mode, or on the
  destination after `VACUUM INTO`.

### 4. The `-wal` sidecar keeps its high-water size

- **Status:** confirmed, reproduced.
- **Location:** `apply_connection_pragmas`; no `journal_size_limit` is set
  anywhere in `src/`.
- **Probe:**
  - after one 20 MiB seal, the `-wal` stayed at 21,218,032 bytes through
    13 s of checkpoints;
  - after a held reader snapshot was released, it stayed at 133 MB until
    `join`.
- **Proposed fix:** `PRAGMA journal_size_limit = 4194304`. The failure-side
  reviewer applied it in their scratch copy and saw the sidecar return to
  4,194,304 bytes at the first checkpoint after the snapshot was released.

### 5. Ticks dropped on a failing disk are reported as `Ok`

- **Status:** confirmed. The reviewer ran it on an 8 MiB RAM disk; the
  auditor traced the reset sites.
- **Location:**
  - `writer.rs:1777` (`WriterHealth::dropped`);
  - the counter resets at `writer.rs:2764` (a clock-offset commit) and
    `writer.rs:2829` (a caller-row commit).
- **What happens:**
  - Up to 30 dropped ticks plus 1 queued return `Ok` before an error
    surfaces, and no counter a caller can read records them.
  - Any successful commit that is not a tick resets the consecutive count,
    so total loss is unbounded when failures alternate with such commits.
- **Probe:** `wal()` returned `Ok` for ts 851–881, and none of those rows is
  in the file. The error first appeared on ts 882.
- **Docs:** FORMAT §1's "at most the two ticks in flight" holds only for an
  unclean kill.
- **Proposed fix:**
  - add a dropped-tick counter with an accessor;
  - reset the consecutive count only on a tick commit;
  - state the bound in FORMAT §1.

### 6. A panic in a caller closure kills the writer silently

- **Status:** confirmed, reproduced.
- **Location:** `sqlite.rs:3208` and `:3170` call a `StreamFilter`, and
  `:433` calls a `CallerRowFloorFn`, on the writer thread, with no
  `catch_unwind`.
- **Probe:** `evict` returned `Err(WriterGone)` with no cause. The next
  `wal` returned `Ok` and its row was lost. `wal(vec![])` and `seal(vec![])`
  kept returning `Ok`. The archive stayed consistent.
- **Docs:** `writer.rs:20` says "The writer is panic-free".
- **Proposed fix:** catch the panic around `writer_loop`, or around the
  callbacks, and fill the error slot.

### 7. Naming one stream twice in a seal batch stops the writer

- **Status:** confirmed, reproduced.
- **Location:** `seal_capped` (`writer.rs:1147`) and `seal_batch`
  (`writer.rs:3437`).
- **What happens:** both entries read the same `first_seq`, the second
  insert hits the primary key, and the error is not retryable. The writer
  exits for every source. The file is unchanged.
- **Proposed fix:** deduplicate the batch.

### 8. `add_source` and `resume_source` are several separate commits

- **Status:** confirmed. Resume was reproduced on a full disk; add was
  traced.
- **Location:** `writer.rs:2122-2123` (resume), and around `writer.rs:2369`
  (add).
- **What happens:**
  - A failed resume returns `Err` after `complete` was already cleared, so a
    finalized source reads as possibly incomplete.
  - A failed add, or a kill partway through it, leaves a source with no
    `writer_sessions` entry and no `encoder` key. `check_encoder` then
    accepts any encoder for that source, and a retry fails with
    `DuplicateSource`.
- **Proposed fix:** one transaction for each sequence.

### 9. A seal that keeps failing is never surfaced

- **Status:** confirmed. The reviewer ran it on the RAM disk; the auditor
  read `writer.rs:2457`.
- **What happens:** a retryable seal error defers the stream with a warning
  and nothing else. No count is kept, and the live tail grows until ticks
  fail too. The probe built 888 live rows this way.
- **Proposed fix:** a per-stream deferred-seal count in `WriterHealth`, and
  optionally a limit after which the writer reports an error.

### 10. A kill during create leaves a file nothing can open or recreate

- **Status:** plausible. The code path was traced; no kill was staged.
- **Location:** `sqlite.rs:862-867` and `stamp_header`.
- **What happens:** the three schema batches and the two header pragmas
  commit separately. A file killed in between is refused by every open,
  as `NotAnArchive` or `UnsupportedSchema { found: 0 }`, and `create`
  refuses it because it exists.
- **Proposed fix:** one `BEGIN … COMMIT` around the schema and the stamp.

### 11. `synchronous=FULL` is not power-loss durable on macOS

- **Status:** plausible. This is SQLite's documented `fullfsync` default; no
  power cut was staged.
- **What happens:** the bundled SQLite leaves `fullfsync` off, so a commit
  is a plain `fsync`, which on macOS does not flush the drive cache.
  - `docs/journal/2026-11-14-the-live-tail.md:114` already notes this.
  - The comment at `sqlite.rs:1256` and FORMAT.md:69 claim more.
- **Proposed fix:** on Apple targets, set `fullfsync` and
  `checkpoint_fullfsync`, or narrow both claims.

### 12. `open_finished_read_write` disables checkpoint-on-close late

- **Status:** plausible; the trigger is improbable.
- **Location:** `sqlite.rs:1051-1060`.
- **What happens:** four fallible steps run before
  `NO_CKPT_ON_CLOSE` is set. If one of them fails, a read-write connection
  closes with checkpoint-on-close on.
- **Proposed fix:** set it immediately after the open, as `open_with_cache`
  does.

### 13. `wal_tick`'s fallback gives up cross-source atomicity silently

- **Status:** confirmed, reproduced.
- **Location:** `writer.rs:1976-2009`.
- **What happens:**
  - After a constraint error, the tick is committed one source at a time.
    `wal_tick` and `insert_wal_rows_batch` document a single transaction.
  - A tick naming an unknown source id fails on a foreign key. That source's
    entry is dropped with a warning that says "collide with rows already
    committed", which names the wrong cause.
- **Proposed fix:**
  - document the fallback;
  - name the constraint in the warning.

### 14. A `seq` above `i64::MAX` wraps to a negative value

- **Status:** confirmed by the reviewer's run.
- **Location:** `seq as i64` at six bind sites in `sqlite.rs`.
- **What happens:** the value is reachable only through the public
  `insert_segment` and `Transaction` API. The stored segment is then read
  first.
- **Proposed fix:** `i64::try_from` and refuse the value.

### 15. A seal deletes an encoder's dropped leading run

- **Status:** confirmed, reproduced. This is also a design question.
- **Location:** `writer.rs:3487` deletes up to `last_ts`.
- **What happens:** rows ahead of the encoder's first anchor are deleted in
  the seal and counted nowhere. Copies omit them too (`rewrite.rs:943`).
  The docs say a dropped trailing run stays live, and say nothing about a
  leading one.
- **Probe:** rows 1–5 went in with the anchor at 3. The result was segment
  `[3, 5]`, an empty WAL, and `dropped_out_of_order` 0.

### 16. `clock_offsets` can hold pairs that belong to no row

- **Status:** confirmed. This is a wording question.
- **What happens:** `finalize((ts, off))` and `clock_offset` store the
  caller's pair unchecked. FORMAT §3.4 does not claim otherwise; invariant
  9, as worded, does.

### 17. Per-stream retention reads one table page per segment

- **Status:** confirmed. Measured on a synthetic 8,000-segment file.
- **Location:** `sqlite.rs:3257-3262` and `oldest_row` (`:409`).
- **What happens:** `MIN(first_ts)` is in no index. The statement took 1,036
  page misses. Taking the minimum from each stream's lowest-`seq` segment
  took 39.
- **Proposed fix:** that rewrite, which is valid by invariant 5.

### 18. Catalog reads are not index-only

- **Status:** confirmed on the same synthetic file. This is a design
  question.
- **What happens:** `read::catalog` uses `segment_span` and `stream_bytes`.
  They read each segment's table leaf page: 1,034 misses for 8,000
  segments.
- **Possible change:** a covering index
  `(source_id, stream, seq, first_ts, last_ts, rows)` on new files, with no
  version bump. Not measured on a real archive.

### Docs against code (invariant 15)

All confirmed unless noted. The reviewers' proposed replacement text was
not kept; each item summarizes it.

- **d1.** FORMAT.md:502, DESIGN.md:316 and the `read_snapshot` comment say a
  held snapshot stops a checkpoint "from moving anything". In fact it stops
  the checkpoint at the snapshot's commit: `wal_checkpoint` returned (busy 0, 22 frames in the log, 12
  checkpointed), so it moved
  the 12 frames the reader could see. FORMAT §2.2's "a plain copy is at
  most that stale" needs the snapshot caveat.
- **d2.** FORMAT §4.6 and DESIGN.md:246 say a read-only open of an unfinished
  archive "leaves the files as they are". It creates `-wal` and `-shm` and
  leaves them. DESIGN's own table says so.
- **d3.** FORMAT §2.1 says `journal_mode` is persistent WAL. Copies are not
  in WAL mode (finding 3).
- **d4.** FORMAT §5 states the time model as a property of the format. The
  crate stores `ts` and `wall_offset` as given and checks only their order.
- **d5.** FORMAT.md:90 says a catalog-less copy is refused "with a message
  naming the cause". The message names `.rez`/rezolus instead.
- **d6.** §2.1's pragma claims:
  - `open_finished_read_write` also sets `synchronous=FULL`;
  - `ArchiveMut::open` gets the reader's 256 MiB cache.
- **d7.** DESIGN's unclean-kill numbers are stale. Measured 3 times now:
  76 KiB archive, 3,345,472-byte sidecar, 92 KiB after the fold. DESIGN has
  64/80 KiB. The same numbers appear at `sqlite.rs:955`.
- **d8.** FORMAT §7's resume floor counts only stream bounds (`kind` 0).
  `resumed_after_ts` excludes `bounds` and can be `null`.
- **d9.** The "NULL uuid predates the column" text describes no file the
  open gate accepts. The column and the stamp landed in one commit
  (c91b5af). On such a file `insert_source_sql` fails with a raw
  `no such column: uuid`.
- **d10.** Comments overstate indexes:
  - `sqlite.rs:519` says the archive "carries one index";
  - `sqlite.rs:3063` and the `SCHEMA_SQL` comment say `segments_by_time`
    serves whole-source retention. It uses only the `source_id` prefix.
- **d11.** The warning at `writer.rs:2669` says a stale watermark drops rows
  "until … a restart re-reads it". Rows are refused by `newest`, which comes
  from `bounds`, so a restart does not change it. The watermark cache is
  read only by replication paths.
- **d12.** The `remove_archive` comment (`sqlite.rs:764`) says a stray
  sidecar is replayed into a new database. Measured: SQLite discards it.
- **d13.** Gaps for a reader built from FORMAT alone:
  - segment bytes are what the encoder returned, and are not checked to be
    Parquet;
  - `[first_ts, last_ts]` is inclusive, and `rows` counts every row in it;
  - live rows go to the encoder in ascending `ts`.

## Design questions for the owner

- **Loss bound under a failing disk** (finding 5). The asynchronous hand-off
  returns `Ok` before the commit. Should a dropped tick be visible per call,
  through a counter, or only as a documented bound?
- **Leading-run deletion** (finding 15). Keep the deletion and document it,
  or keep those rows live until a later seal can place them?
- **Invariant 9** (finding 16). Restate it, or have `finalize` derive its
  observation from the newest row?
- **Schema-held rules.** The code alone holds `first_ts <= last_ts`,
  `rows > 0`, time order across segments, and documents being append-only.
  `CHECK` constraints on new files could hold the first two.
- **Covering catalog index** (finding 18).
- **`writer_sessions` and `events`** grow by one entry per resume and are
  rewritten whole each time. This is small in practice and not documented.

## Checked and holding

Recorded so they are not raised again:

- Seal atomicity, and that nothing changes a stream between `seal_batch`'s
  read and its write. One writer thread; `-lock` refuses other
  `ArchiveMut`s.
- Retention never un-shadows a covered row (control test above).
- Compaction and copy keep `seq`/time order, the seam and `bounds`. A copy
  does not carry covered pre-0.8.0 WAL rows.
- `refresh_newest` never leaves the cache below the file.
- No source is ever deleted. Every writing connection has
  `foreign_keys=ON`.
- `caller_rows` rowid values are never depended on, only their relative
  order.
- Creating `bounds` inside a delete transaction is safe for readers holding
  a snapshot. No `prepare_cached` exists.
- Every deferred transaction in the writer starts with a write, and the
  read-then-write paths use `transaction_immediate`.
- `wal_autocheckpoint` is derived from the file's own `page_size`.
- A missing or stale `-shm` after SIGKILL recovers every row.
- After ENOSPC is cleared, the file passes `integrity_check` and resumes
  normally.
- Finalize is one transaction.

## Not covered

- Replication, WIRE.md and the writer's replication paths (out of scope).
- IOERR injected at commit (no fault VFS on macOS), and Linux fsync-error
  semantics.
- A kill during a seal transaction, a checkpoint or `incremental_vacuum`.
  No existing test does this either: `unclean_kill.rs` kills only after
  `sync()`.
- Power loss, and Windows (`LockFileEx` does not lose locks the way finding
  1 does).
- The immutable-fallback trigger with two real user accounts, and a `-shm`
  that exists but is not writable.
- Free-list behaviour in steady state under retention.
- Numbers measured in rezolus and cited by DESIGN.md (the 3.14x sweep, the
  8.230 vs 8.807 ms figures, 123 missing appends, 16 of 26 streams).
- The tests in `sqlite.rs` (lines 4354 onward) were not reviewed as tests.
  Two reviewers read `read.rs` and `seal.rs` only in part.
