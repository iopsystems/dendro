---
status: implemented
opened: 2026-10-01
updated: 2026-10-01
---

# Writer heartbeat: telling a source being written from one whose writer stopped

## Goal

A reader of a source with `complete = 0` should be able to tell which of
three things it holds: a source a writer is still appending to, one whose
writer was killed, or a copy of a running archive. `complete` says only
that the writer did not finalize.

## Why

rezolus's viewer follows an archive file that is still being written
(rezolus #1390): it reopens the file and shows new rows as they land. Its
first rule for when to stop following was a stall bound, ten sampling
intervals and at least 30 s without a new row. A recording sampled once a
minute broke it: the reader's measured interval falls back to 1 s before it
has two rows, so the bound was 30 s and the follow ended between the first
two samples. Any bound derived from row times has this problem, because row
cadence is the producer's choice and can be anything. Nothing else in the
file says whether a writer is running, and dendro's live writer holds no
lock a reader could test (the only exclusive lock is `ArchiveMut`'s, for
offline edits).

## Design

- Two nullable columns on `sources`: `heartbeat` (a counter) and
  `heartbeat_interval_ns`. A nullable column an old reader ignores needs no
  schema version change (FORMAT.md §8).
- The writer thread sets the interval when it adds or resumes a source and
  bumps every open source's heartbeat on its own timer
  (`writer::HEARTBEAT_INTERVAL`, 5 s), in one small transaction, until the
  source is finalized. The timer shares the loop's `recv_timeout` with the
  checkpoint timer, so an idle writer still beats; a busy one beats on the
  same timer rather than per commit, which keeps the commit path unchanged.
  A failed beat is logged and skipped.
- A reader decides with `archive::HeartbeatWatch`: the heartbeat unchanged
  for three intervals of the reader's clock is a stopped writer. It compares
  two values the file held, so clock skew between the writer's host and the
  reader's does not enter.
- Copies carry both columns unchanged. No writer bumps the copy, so it
  reads as stopped after three intervals, which is correct: nothing will
  append to it.

## Alternatives not taken

- **A lock the writer holds** (`flock` on a sidecar). It is released the
  moment the writer dies and needs no clock, but advisory locks are
  unreliable on network filesystems, and a reader of uploaded bytes (the
  browser viewer) cannot see one.
- **A wall-clock timestamp of the last commit**, compared with the reader's
  clock. Simpler to read once, but wrong under clock skew between hosts, and
  a writer whose producer samples rarely still needs an idle timer to keep
  it fresh.

## Outcome

Implemented with `tests/heartbeat.rs`: an idle writer beats and reads live;
a writer that goes away without finalizing reads stopped; a copy of a
running source reads stopped while the original stays live; a file without
the columns reads unknown until a writer reopens it, adds them and beats.
Cost: one `UPDATE` per open source every 5 s.

## Reopen

If a reader needs liveness from a single read rather than across reads (no
second look possible), add a wall-clock stamp beside the counter and accept
the skew it brings.
