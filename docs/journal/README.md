# Engineering Journal

One durable, repository-local record per non-trivial effort. Entry frontmatter
is authoritative for lifecycle status.

The map, if you are new: [what a TSDB has that we do
not](2026-09-12-what-a-tsdb-has-that-we-do-not.md) classifies every absence
as ours or not ours, and links the entries that own each one.

| Opened | Effort | Status |
| --- | --- | --- |
| 2026-09-12 | [What a TSDB has that we do not, and which of it is ours to build](2026-09-12-what-a-tsdb-has-that-we-do-not.md) | implemented (4 of 5; compaction gated) |
| 2026-09-12 | [Generations: telling a counter reset from a counter wrap](2026-09-12-generations-reset-versus-wrap.md) | open |
| 2026-09-11 | [Container hardening: failure classes, identity, sessions, and one materialization](2026-09-11-container-hardening.md) | implemented |
| 2026-09-11 | [Segment compaction](2026-09-11-segment-compaction.md) | open |
| 2026-09-11 | [Out-of-order appends are accepted and never read](2026-09-11-out-of-order-appends.md) | partly resolved (loud; backfill open) |
| 2026-09-11 | [One source's bad tick kills the writer for the whole archive](2026-09-11-writer-failure-blast-radius.md) | resolved (container hardening, item 2) |
| 2026-09-11 | [The encoder boundary does not yet earn the crate's generality claim](2026-09-11-encoder-boundary.md) | open |

Two kinds of entry live here. Some record **work**: container hardening and
the survey above are efforts with commits behind them. The rest record a
**gap** — a limitation nobody is building, written down once with a reopen
condition, so that the reason it is hard does not have to be re-derived.

The gap entries came out of an adversarial review on 2026-09-11 that also
found six defects, fixed rather than filed: a read path with no snapshot,
`u64 as i64` at six of seven binding sites, a prune that deleted rows the
encoder had not encoded, `Db::open` silently rewriting the archive, unbounded
`clock_offsets`, and retention unreachable through the supported API. A
second review the same day produced the container-hardening entry, which
resolved the writer blast radius and the loudness half of out-of-order
appends.

`README.md` names the reader-facing gaps under "Known gaps"; `FORMAT.md`
specifies the format itself.
