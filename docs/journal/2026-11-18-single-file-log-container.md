---
status: open
opened: 2026-11-18
updated: 2026-11-19
---

# One file in every state: an append-only log in place of SQLite

## Goal

Decide whether dendro's container should become one file in every state,
open, closed, crashed or being read, with no sidecar files. Today an archive
is one file only after a clean close. While it is open it also has `-wal` and
`-shm`, after a crash it has all three, and every write open leaves
`-lock` beside it (DESIGN.md, "How many files an archive is").

The owner wants one artifact in every state. SQLite cannot provide that with
any journal mode, so the question is whether a different container can, at
a cost the owner accepts. This entry is the GO/NO-GO probe. It writes no code
in `src/`.

## Why SQLite cannot be configured into one file

| SQLite mode | extra files while open | after a crash | readers while writing |
|---|---|---|---|
| WAL (today) | `-wal`, `-shm` | `-wal`, `-shm`; deleting `-wal` loses commits | yes; readers do not block the writer |
| WAL, `locking_mode=EXCLUSIVE` | `-wal` | `-wal` | no |
| rollback journal | `-journal` during each write (`DELETE`); a `-journal` that stays after the first write (`TRUNCATE`, `PERSIST`) | a hot `-journal`; deleting it corrupts the file | readers block commits |
| `journal_mode=OFF` or `MEMORY` | none | a kill mid-commit can corrupt the file | readers block commits |

Every mode either leaves a sidecar file beside the archive or loses one of
three properties FORMAT.md §1 relies on: a durable commit per append, a
valid file at every instant, and reads while the writer commits. This is SQLite's documented behaviour
(sqlite.org/wal.html, sqlite.org/lockingv3.html), not a dendro measurement.

The `-lock` file is a consequence of SQLite too. dendro cannot `flock` the
archive itself because SQLite's POSIX locks conflict with `flock` on macOS,
and on unix a `close()` of any descriptor on the archive drops every POSIX
lock the process holds on it (DESIGN.md, "One writing connection"). The
storage audit's finding 1 (2026-11-18-storage-audit.md) is that second
property: `sniff` closes a descriptor in the writer's process, SQLite's lock
is gone, and a refused `ArchiveMut::open` checkpoints and deletes a live
`-wal`. Reproduced in a scratch probe; committed rows were lost.

## The candidate

One file, written only by appending. Every commit is one record: a header
(kind, length), a payload, and a checksum over both, followed by
`fdatasync`.

- **Tick:** one record holding the tick's rows.
- **Seal:** one record holding the Parquet segment, its catalog entry, the
  clock observation, and the watermark that retires the WAL rows it covers.
  One record is one commit, so seal atomicity (audit invariant 3) is the
  record's checksum.
- **Recovery:** read forward to the last record whose checksum verifies. A
  torn tail is ignored by every reader and overwritten by the next writer.
  Nothing has to be repaired before a read.
- **Readers:** read up to the last valid record. No shared memory, so the
  one-host requirement (DESIGN.md cost 1) may relax for readers. Unchecked:
  whether a reader on NFS or SMB sees appends in order.
- **Lock:** the writer takes `flock` on the archive itself. With no POSIX
  locks on the file, nothing conflicts with it and no `close()` drops it.
  No `-lock` file.
- **Catalog:** periodic index records. Open seeks to the newest index record
  and scans the records after it.
- **Retention:** discussed under Criteria; this is the part most likely to
  fail.

### What it costs (given, not measured)

1. **Retention without `auto_vacuum`.** An append-only file does not return
   space on its own. The options are hole punching (`fallocate`
   `FALLOC_FL_PUNCH_HOLE` on Linux, `fcntl(F_PUNCHHOLE)` on APFS), a
   preallocated ring, or rewrite-and-rename. Rewrite-and-rename replaces the
   inode, so a reader following the live file loses it.
2. **Every in-place edit becomes an appended record.** Metadata patches,
   `bounds`, deleting WAL rows at a seal, compaction and documents all
   become records, and space comes back only through retention or a rewrite.
   The rules the audit checked (the watermark, `bounds`, `seq` order) must be
   rebuilt on the new container.
3. **No SQL catalog.** `src/archive/sqlite.rs` is 5,763 lines. The read
   paths, `verify`, copy, compaction, `open_bytes` (the browser reader) and
   rezolus's reader depend on it. This is a new format version with a
   copy-forward from v4 and v5, not an edit.
4. **A hole-punched file's apparent size grows without bound.** `st_size`
   keeps rising while allocated blocks stay bounded. A tool that copies
   without sparse support (`cp` on some platforms, an upload) copies the
   holes as zeros. A ring avoids this at the cost of a fixed preallocation.

## Decision Criteria

Written before the probe ran.

**G1, tick commit cost.** GO when, on each of APFS (macOS), ext4 and xfs
(Linux), the median per-tick time of `write` + `fdatasync` of one record is
no greater than the median per-tick time of today's tick commit on the same
input, over at least 15 interleaved trials per arm, beyond the difference
between the medians of two identical arms of today's commit (the A-vs-A
floor).

- Today's commit is SQLite at dendro's settings: WAL mode, `page_size=4096`,
  `synchronous=FULL`, `wal_autocheckpoint` of 4 MiB of pages, one
  transaction inserting a tick's rows into the v5 `wal` table, a PASSIVE
  checkpoint every 10 s.
- Input: DESIGN.md's mixed workload, 26 streams of 1,925-byte rows, one
  commit per tick.
- On macOS both arms use `fsync`, matching today's SQLite, which does not
  set `fullfsync` (audit finding 11). Both arms are also run with
  `F_FULLFSYNC` and reported, not used for the verdict.
- Prediction: the log arm is faster, because SQLite's commit writes 3.14x
  the row payload to `-wal` (DESIGN.md, "What a row costs to write") and the
  log writes about 1.0x. A log arm no faster than SQLite beyond the floor
  refutes that the write volume is what costs time per commit; the criterion
  then turns on the fsync alone.

**G2, bounded retention by hole punching.** GO when, on each of APFS, ext4
and xfs, a rolling buffer keeps its allocated size bounded:

- Append records of the G1 workload plus a 4 MiB seal record every 1,000
  ticks. Hold a working set of 256 MiB: once the live span passes it, punch
  the oldest records' whole filesystem blocks.
- Run until 20x the working set (5 GiB) has been appended.
- Bounded means allocated bytes (`st_blocks` x 512) at the end are at most
  1.10x the live bytes plus one filesystem block per punched region
  boundary, and allocated bytes at 10x and 20x differ by at most 2%.
- Report also: the extent count at the end (`filefrag` on Linux), and the
  median and p99 time of one punch call.
- A reader opened at the start and following the file must read every
  record that was live when it read it, and must see zeros, never another
  record's bytes, where a punch landed.

**NO-GO** if any of the three filesystems cannot keep the bound by hole
punching, so that retention would need rewrite-and-rename. Reopen if a ring
layout is designed whose fixed preallocation the owner accepts.

**Not decided by this probe:** read throughput against today's catalog
queries, the open time of a large archive (index records), compaction, how
readers behave on network filesystems, and Windows. A GO here means the
format is worth designing, not that it ships.

## Scope

In: the container's file layout, commit, recovery, locking and retention.

Out: the encoder, segment format (Parquet stays), the replication wire, and
the audit's other findings. Findings 1 (lock ordering) and 4 (no
`journal_size_limit`) need fixing on the current format whatever this probe
decides, because the current format ships until a replacement does.

## Plan

1. A standalone probe crate, outside the crate's workspace: the SQLite arm
   is dendro's own writer connection, and the log arm is `write` +
   `fdatasync`. Its source is in
   [2026-11-18-single-file-log-container/](2026-11-18-single-file-log-container/),
   with the patch it needs and the commands that build and run it.
2. G1 on macOS APFS, then in a Linux VM on the rack on ext4 and xfs.
3. G2 on the same three filesystems.
4. Record the numbers, the commands and the verdict here.

## Evidence

### The probe

A standalone crate in
[2026-11-18-single-file-log-container/](2026-11-18-single-file-log-container/),
outside the workspace. It depends on dendro at a7000b5 with
one scratch patch: `ArchiveMut::open_for_write` made `pub`, and a
`probe_pragma` method added so the SQLite arm can set `fullfsync`. The
SQLite arm is therefore the writer thread's own connection and its own
`insert_wal_rows_batch`, not a re-implementation.

- **G1:** each trial runs every arm on a fresh file: 200 warm-up ticks, then
  1,000 timed ticks. A tick is 26 rows of 1,925 bytes from a fixed seed. The
  arms rotate their start position each trial. Arms: `sqlite_a`, `sqlite_b`
  (identical; their difference is the floor), `log_a`, `log_b`, and on
  macOS `sqlite_full` and `log_full` with `F_FULLFSYNC`. The log arm's time
  includes encoding the record and its CRC32, and also generating the
  tick's rows, which the SQLite arm does before its timer starts; that
  biases against the log arm. The SQLite arm's time includes binding. The figure per arm is the median over trials of each trial's
  median per-tick time.
- **G2:** one writer appends ticks and a 4 MiB seal record every 1,000
  ticks, each followed by `fsync` (`fdatasync` on Linux). Once a live span
  exceeds 256 MiB it drops the oldest records and punches whole blocks below
  the new live start, in punches of at least 1 MiB. A reader thread follows
  the file from the start, sleeps 4 s every 20,000 records so punches land
  under it, and checks each record's bytes against a regeneration from its
  sequence number. A byte that differs is allowed only if it is zero and
  below the punch frontier; anything else counts as a violation.
- **The violation check was verified by mutation:** with `PROBE_CORRUPT`
  set, the writer overwrites 16 bytes of a live record 1,000 records back.
  The reader reported `VIOLATIONS 1` at that record's offset.

### macOS (APFS), Apple M4 Max, macOS 26.6.2

Measured, 15 trials, per-tick microseconds:

| arm | median of trial medians | range of trial medians | median p99 |
|---|---|---|---|
| `sqlite_a` | 222.8 | 205.7–287.0 | 3,487.0 |
| `sqlite_b` | 220.3 | 204.4–317.5 | 3,384.8 |
| `log_a` | 71.2 | 67.8–85.7 | 100.0 |
| `log_b` | 71.7 | 66.9–88.1 | 105.0 |
| `sqlite_full` | 4,254.3 | 4,214.2–4,325.1 | 17,304.3 |
| `log_full` | 4,236.9 | 4,199.5–4,300.7 | 6,798.9 |

- **G1 holds on APFS.** The A-vs-A floor is 2.5 us (222.8 against 220.3).
  The log arm is 151 us faster per tick, a factor of 3.1, and the trial
  ranges do not overlap.
- **The prediction held.** The probe did not count bytes written, so this
  does not show that write volume is the cause.
- **`F_FULLFSYNC` erases the median difference.** Both arms take about
  4.25 ms, so the cache flush is the whole cost. The log arm's p99 is 2.5x
  lower (6.8 ms against 17.3 ms). The SQLite tail is probably the commits
  that run a 4 MiB autocheckpoint; the probe did not attribute it.
- **G2 holds on APFS.** Allocated bytes over live bytes are 1.0370 at 10x
  (2,560 MiB appended) and 1.0361 at 20x (5,120 MiB), well under 1.10x, and
  differ by 0.09%. The apparent size grows with the bytes appended (5,120
  MiB at the end), as cost 4 says.
- 4,172 punches took a median of 27 us and a p99 of 1,528 us.
- The reader read 24,868 records whole and 72,742 fully punched, one partly
  punched, and 0 violations.

### Linux (ext4, xfs)

The first run put both filesystems in one job and timed out at 5,400 s
during xfs G2. The upload steps never ran, so it produced no numbers. Rerun
as one job per filesystem with a 10,800 s limit.

Setup: Debian 13 guests (`debian-13-6.12`, image build 20261113.1, kernel
6.12.116), 24 vCPU, with ext4 on hv02, a Zen 1 host (systemslab experiment
01a1f2f4-4e26-7196-55d9-9debff780f2f) and xfs on hv01, a Zen 2 host (experiment
01a1f2f4-4eba-719a-0fb7-79f7b0be970d). Each filesystem is made fresh on a
24 GiB loop device (`--direct-io=on`), backed by a file on the guest's ext4
root disk `vda`, which reports `write back`. A guest `fdatasync` therefore
goes through the loop device and virtio to the host's storage. Each tick
takes 15.5 to 18.8 ms on this stack, about 73 to 84 times the Mac's SQLite
arm and 219 to 253 times its log arm; the probe did not time `fdatasync`
separately. The time follows the host rather than the filesystem: the
SQLite arm took 18.7 and 18.8 ms on hv02 and 16.2 and 16.6 ms on hv01,
counting the follow-up runs below, which swap the hosts. The absolute times describe
this stack, not bare-metal disks; the comparison between arms holds because
both arms ran on the same stack, interleaved.

Measured, 15 trials, per-tick microseconds, median of trial medians:

| arm | ext4 | ext4 trial range | xfs | xfs trial range |
|---|---|---|---|---|
| `sqlite_a` | 18,704.0 | 9,474.8–19,003.2 | 16,216.1 | 6,553.5–16,321.8 |
| `sqlite_b` | 18,724.4 | 9,543.3–19,022.5 | 16,257.2 | 6,404.3–16,322.0 |
| `log_a` | 18,027.5 | 7,950.1–18,245.7 | 15,565.3 | 4,831.5–15,674.2 |
| `log_b` | 18,024.3 | 7,629.7–18,210.4 | 15,536.5 | 4,902.6–15,629.6 |

- **G1 holds on ext4 and xfs.** On ext4 the log is 677 us faster (3.6%)
  against a floor of 20 us. On xfs it is 651 us faster (4.0%) against a
  floor of 41 us.
- **The sync probably dominates on this stack,** inferred from the small
  relative difference; it was not timed. The absolute saving is about 4x
  the Mac's 151 us. I don't know why; the probe did not measure where
  inside the commit the time goes.
- **The first trials of each job ran fast.** Trial 0 of each job, and on
  xfs the first two arms of trial 1, ran at 31% to 54% of the others'
  per-tick time, in every arm that ran in that window: about the first 45 s
  of the job. I don't know why. The trial ranges in the table include them.

G2, allocated over live bytes:

| appended | ext4 | xfs |
|---|---|---|
| 2,560 MiB (10x) | 1.0010 | 1.3300 |
| 5,120 MiB (20x) | 1.0002 | 2.9884 |
| range over the run | 1.0000–1.0038 | 1.0931–2.9884 |

- **G2 holds on ext4.** Punches took a median of 233 us and a p99 of
  957 us.
- **G2 fails on xfs as written.** Allocated bytes swing between 275.8 and
  765.0 MiB against 256 MiB live, and the excess does not trend with bytes
  appended. Punches took a median of 133 us and a p99 of 473 us.
- **Extent counts were not collected.** The criterion asked for them, but
  `filefrag` produced no output in any Linux job, and the probe discards a
  failed command silently.
- **The Linux reader checks prove nothing.** With 16 to 18 ms per sync the
  writer never got 256 MiB ahead of the reader. The reader read all 97,611
  records whole and none punched, so the punched-region check never ran.

### Follow-up: what xfs's excess is (prediction written before running)

Hypothesis: the excess is xfs speculative preallocation past end-of-file,
which xfs sizes in proportion to the file's size. It is not
punched-but-retained space.

- Prediction: with the file kept, the blocks `xfs_bmap` reports beyond EOF
  roughly equal allocated minus live; mounted with `allocsize=64k`, which
  fixes the preallocation size, the ratio stays under 1.10x at 10x and 20x.
- Refutation: under `allocsize=64k` the ratio still exceeds 1.10x, or the
  excess lies below EOF. That would mean xfs does not free the punched
  ranges, which is the NO-GO case.
- In the same job, ext4 and xfs are rerun with `--no-sync`. The writer then
  outruns the reader, so the punched-region check runs on Linux.

Result: hv02, experiment 01a1f329-da5c-71bb-cfe1-b0e8dbae7313, same image
and loop setup.

- **A, xfs default mount, sync, 10x, file kept.** While the file was open,
  the ratio swung between 1.22x and 3.00x, ending at 1.75x, as before.
  After close, the file holds 268,677,120 allocated bytes against
  268,420,552 live, 1.0010x. `xfs_bmap` shows one hole followed by one
  256.2 MiB extent, and nothing past EOF. A sync and a cache drop changed
  nothing.
- **What A did not show.** `xfs_bmap` ran after close, so it never saw the
  extent past EOF while the file was open. That the excess was post-EOF
  preallocation is inferred from close releasing it and from B.
- **B, xfs `allocsize=64k`, sync, 20x.** The ratio is 1.0012 at 10x and
  1.0003 at 20x, with a range of 1.0000 to 1.0039 over the run. That is the
  same curve as ext4. The prediction held: fixing the preallocation size
  removes the excess, so xfs does free the punched ranges.
- **C, ext4, no sync, 10x.** The reader read 24,872 records whole and
  23,973 fully punched, one partly punched, and 0 violations.
- **D, xfs default, no sync, 10x.** The reader read 24,872 records whole and
  23,973 fully punched, one partly punched, and 0 violations. Punches took a
  median of 1,278 us and a p99 of 3,431 us, about 10x their cost with sync.

## Verdict on the criteria

| | APFS | ext4 | xfs |
|---|---|---|---|
| G1, tick commit | GO, 3.1x faster | GO, 3.6% faster | GO, 4.0% faster |
| G2 as written | GO | GO | fails at the default mount: up to 3.0x while open |
| G2, `allocsize=64k` | | | GO |
| G2, 16 MiB chunks, set by the writer | GO, 1.0625 | GO, 1.0625 | GO, 1.0625 |
| G2, extent size hint, set by the writer | | | GO, 1.0039 |
| reader sees only its bytes or zeros | yes | yes | yes |

- **G1 passes everywhere.** The log never costs more per tick than today's
  commit. Where `fsync` is cheap (APFS) it is 3.1x faster; where `fsync`
  dominates (this VM stack) the difference is a few percent.
- **Hole punching frees space on all three filesystems.** The NO-GO
  condition, retention that needs rewrite-and-rename, is not met.
- **G2 as written fails on default-mounted xfs.** While the file is open,
  xfs holds up to 2x the working set beyond the live data, and releases it
  at close. That this is speculative preallocation past EOF is inferred
  from A, B and the hint run below, not observed. A mount option fixes it,
  but dendro cannot require a mount option of its users.
- **Open question for the owner, after the first xfs run.** Either accept a
  transient overshoot of up to 3x on xfs, or have the writer control
  preallocation for its own file. The follow-up below tested two per-file
  controls, and both hold G2 on xfs.

Status stays `open`: the probe finished, and the decision to design the
format is the owner's.

### Follow-up: the writer controls its own preallocation

The owner's direction (2026-11-19): find out whether xfs has an extent size
hint that works; otherwise preallocate in fixed chunks, which the owner
thought probably good for several reasons.

Two variants, written before running:

- **Extent size hint (xfs only).** `FS_IOC_FSSETXATTR` with
  `FS_XFLAG_EXTSIZE` and `fsx_extsize` = 1 MiB, set on the empty file and
  read back. Expectation, from memory of xfs's buffered-write path and not
  checked: xfs does not use delayed allocation for a file with a hint, so no
  speculative preallocation happens.
- **Chunked preallocation (all three).** Before each append, if the record
  would cross the allocated end, allocate the next 16 MiB without moving
  EOF: `fallocate(FALLOC_FL_KEEP_SIZE)` on Linux, and
  `fcntl(F_PREALLOCATE, F_ALLOCATEALL, F_PEOFPOSMODE)` on macOS. `O_APPEND`
  still appends after the last record.
- **Bound:** allocated bytes stay below live + 16 MiB (the chunk) + 1 MiB
  (the punch granularity) + one filesystem block: about 1.067x at a 256 MiB
  working set. (As first written this added one record and stated 1.07x;
  with a 4 MiB seal record that sum is 1.082x, and the term is not needed,
  because allocation runs before each write.)
- **Prediction, xfs:** both variants hold G2. The hint gives about 1.00x
  and chunks at most 1.07x, steady from 10x to 20x. A hint run that
  overshoots like the default mount refutes the expectation.
- **Prediction, G1:** neither variant costs more per tick than the plain
  log beyond the floor. A preallocated extent is unwritten, and converting
  it on first write is a metadata change that `fdatasync` must persist, as
  is the size change it saves, so I expect no difference. A slower
  preallocated arm refutes that.
- **Smoke run on APFS (one run, 2 trials, 64 MiB working set, not
  evidence):** chunks held allocated bytes at live + 16 MiB (80.0 to 80.9
  MiB against 64), and the preallocated arm ran 52.9 us per tick against
  63.1 us.

Result:

- **Where it ran.** xfs on hv02 (experiment
  01a1f4e7-0dea-7144-4c9d-1f5547dfbcbc) and ext4 on hv01 (experiment
  01a1f4e7-0e6f-71ff-49ae-62344ed60831). The hosts are swapped from the
  first runs; the same image and loop setup.
- **The hint was set.** It read back as `xflags 0x80000800`, `extsize
  1048576`.

G2, allocated over live bytes, measured:

| variant | 10x | 20x | range over the run |
|---|---|---|---|
| xfs, extent size hint 1 MiB | 1.0048 | 1.0039 | 1.0010–1.0075 |
| xfs, 16 MiB chunks | 1.0634 | 1.0625 | 1.0479–1.0661 |
| ext4, 16 MiB chunks | 1.0634 | 1.0625 | 1.0479–1.0661 |
| APFS, 16 MiB chunks | 1.0634 | 1.0625 | 1.0479–1.0661 |

- **Both variants hold G2, and the prediction held.** The hint's run matches
  `allocsize=64k`.
- **The chunked runs are identical on all three filesystems.** Every row
  reads live + 16 MiB plus up to one 1 MiB punch, because a filesystem then
  allocates exactly what the writer asks for.
- **Readers.** The APFS reader read 72,742 records fully punched and 0
  violations. As before, the synced Linux readers never fell behind into a
  punched range.

G1, per-tick microseconds, median of 15 trial medians:

| arm | APFS | ext4 | xfs |
|---|---|---|---|
| `sqlite_a` | 216.4 | 16,561.0 | 18,791.5 |
| `log_a` | 65.4 | 15,987.8 | 18,030.3 |
| `log_b` | 66.6 | 15,996.8 | 18,063.4 |
| `log_prealloc` | 51.0 | 15,908.0 | 17,995.8 |
| `log_extsize` | | | 17,989.3 |
| floor (`log_a` against `log_b`) | 1.2 | 9.0 | 33.1 |

- **The prediction failed in the favourable direction.** Preallocation
  costs nothing per tick anywhere.
  - On APFS it is 22% faster than the plain log (51.0 against 65.4 us).
  - On ext4 it is 80 us (0.5%) faster, against a 9 us floor.
  - On xfs the preallocated arm is 35 us faster than `log_a` and the hint
    arm 41 us, against a 33 us floor: just outside it, and under 0.25%.
- **Why APFS gains, not shown.** Probably the `fsync` after an append that
  stays inside allocated space does not have to persist an allocation; the
  probe did not trace it.
- **The fast first trials did not recur.** Every Linux arm's range is now
  within 4% of its median. This G1 ran after 27 (ext4) to 60 (xfs) minutes
  of G2, so it never ran in a job's first 45 s; that is probably why
  (inferred).

Both mechanisms work on xfs. Chunks work on all three filesystems, bound
the allocation at the working set plus one chunk whatever the filesystem
would do by itself, and cost nothing per tick. Which one the design uses, and whether xfs
also gets the hint, is the owner's choice and is not yet made.

## Outcome

Open. The probe ran on 2026-11-18 and 2026-11-19; see the verdict above.
Nothing has been designed or built.
