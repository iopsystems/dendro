---
status: resolved
opened: 2026-09-11
updated: 2026-09-11
---

# One source's bad tick kills the writer for the whole archive

## Goal

Decide whether a failure attributable to one source should be able to end the
recording for every source in the archive. Today it can, permanently, and the
source that caused it is told `Ok`.

## Decision Criteria

**GO on per-source isolation** when an archive routinely holds sources that do
not trust each other — several hosts, a mixed fleet, anything where one
producer's bug should not be another's outage. That is the multi-source case
the crate advertises, so the bar is low; what is missing is a decision about
what "isolated" means when the underlying transaction is shared.

**NO-GO on simply not failing.** A writer that swallows errors to stay up is
worse than one that stops: the archive keeps accepting rows it is not storing.

## Scope

In: the fail-stop policy in `writer_loop`, the shared error slot, and whether
a failed source can be quarantined while the others continue.

Out: the encoder contract itself ([the encoder
boundary](2026-09-11-encoder-boundary.md)), and resumption after process exit —
`Archive::create` refuses an existing file and `next_seq` starts at zero, so
"reopen and append" is a separate design.

## Evidence

Measured 2026-09-11. Two sources, A and B. A sends one tick containing two rows
with the same `(stream, ts)` — `wal`'s primary key:

```
A.wal(duplicate ts)         -> Ok(())     <- the culprit is told it succeeded
B.wal after A's bad tick    -> Err("... UNIQUE constraint failed: wal.source_id, wal.stream, wal.ts")
B.wal again                 -> Err(same)
```

B did nothing wrong and is dead for good. The error names A's stream and not
which source, and reaches B through the shared `ErrorSlot`, so an operator
reading B's log sees a constraint violation on a row B never wrote.

The blast radius is wider than duplicate timestamps. Every one of these ends the
archive: a `SQLITE_FULL` that later clears, one stream's encoder returning
`Err`, and — because `insert_wal_rows_batch` is one transaction across sources —
one source's failing row also rolls back every other source's rows for that
tick. Cross-source atomicity is presented as a feature in `DESIGN.md`; combined
with fail-stop it is also a failure amplifier.

Two of the triggers have since been narrowed rather than removed. A retention
failure no longer kills the writer (the caller gets the error and the thread
stays up), and `finalize`/`sync` now surface the stored error instead of
returning `Ok(())` over a dead writer. The duplicate-key and encoder paths still
end it.

Worth recording because it shows the cost is already being paid downstream:
rezolus carries a twelve-line comment naming this exact path and a
`seen_this_tick` HashSet to dedupe before dendro sees it. A container whose only
caller must pre-sanitize input to avoid total failure has put an invariant on
the wrong side of the boundary.

## Design and Implementation

Nothing built. The options, and what each costs:

**Quarantine the source.** On an error attributable to one `source_id`, mark
that source failed, stop accepting its rows, and keep serving the others. Needs
an answer for the shared tick transaction: either ticks stop being atomic across
sources (losing the one-fsync-per-tick property that motivated them), or a
failing source is dropped from the batch and retried alone.

**Make the common trigger not an error.** `INSERT OR IGNORE` on the WAL, with a
returned count of rows dropped. Duplicate timestamps stop being fatal and become
something a caller can observe. Cheap, and it does not address encoder failures
or `SQLITE_FULL`.

**Let the caller choose.** A policy on the archive: fail-stop, or isolate. That
is honest about there being no single right answer — a single-source recorder
genuinely does want to stop — but it is more surface, and the crate has so far
preferred to pick a behaviour and explain it.

## Outcome

Resolved by [container hardening](2026-09-11-container-hardening.md), item 2.
The answer to "what isolated means when the transaction is shared" is: the
tick stays one transaction on the happy path; on a constraint failure it is
re-committed per source, and only the colliding source loses its rows, warned
once. Transient SQLite conditions are retried on a bounded schedule, a seal
that cannot commit is deferred rather than lost, and a run of thirty dropped
ticks stops the writer — so "simply not failing" is still refused. The
encoder-failure trigger is unchanged (fail-stop, now correctly attributed
even when the encoder panics); quarantining a source whose encoder is broken
remains open under [the encoder boundary](2026-09-11-encoder-boundary.md).

## Deferred or Reopen Items

- **Reopen** when an archive holds sources that should not be able to take each
  other down — which is the multi-host case the README already describes.
- Related: [out-of-order appends](2026-09-11-out-of-order-appends.md) shares the
  duplicate-key surface, and [the encoder
  boundary](2026-09-11-encoder-boundary.md) covers the encoder-failure trigger.

## Appendix: Skills Invoked

- `engineering-journal` — this entry.
